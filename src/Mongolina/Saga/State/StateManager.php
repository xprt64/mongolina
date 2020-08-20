<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\Saga\State;


use Dudulina\Saga\State\ProcessStateLoader;
use Dudulina\Saga\State\ProcessStateUpdater;
use InvalidArgumentException;
use MongoDB\BSON\Regex;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\BulkWriteException;
use ReflectionFunction;
use function call_user_func;
use function unserialize;

/**
 * State manager; uses optimistic locking
 */
class StateManager implements ProcessStateUpdater, ProcessStateLoader
{

    /**
     * @var Database
     */
    private $database;
    /**
     * @var Database
     */
    private $adminDatabase;

    public function __construct(
        Database $database,
        Database $adminDatabase
    )
    {
        $this->database = $database;
        $this->adminDatabase = $adminDatabase;
    }

    public function createStorage(string $storageName = 'global_namespace', string $namespace = '')
    {
        $collection = $this->getCollection($storageName, $namespace);
        $collection->createIndex(['stateClass' => 1, 'stateId' => 1,]);
        $collection->createIndex(['stateClass' => 1, 'stateId' => 1, 'version' => -1], ['unique' => true]);
    }

    public function loadState(string $stateClass, $stateId, string $storageName = 'global_namespace', string $namespace = '')
    {
        return $this->loadStateWithVersion($stateClass, $stateId, $version, $storageName, $namespace);
    }

    private function loadStateWithVersion(string $stateClass, $stateId, &$version, string $storageName, string $namespace = '')
    {
        $cursor = $this->getCollection($storageName, $namespace)->find([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
        ], [
            'sort'  => ['version' => -1],
            'limit' => 1,
        ]);

        $documents = $cursor->toArray();

        if ($documents) {
            $document = reset($documents);
            $version = (int)$document['version'];
            return unserialize($document['payload']);
        }

        $version = 0;
        return null;
    }

    public function updateState($stateId, callable $updater, string $storageName = 'global_namespace', string $namespace = '')
    {
        while (true) {
            try {
                $this->tryUpdateState($stateId, $updater, $storageName, $namespace);
                break;
            } catch (BulkWriteException $bulkWriteException) {
                continue;
            }
        }
    }

    public function updateStateIfExists($stateId, callable $updater, string $storageName = 'global_namespace', string $namespace = '')
    {
        while (true) {
            try {
                $this->tryUpdateStateIfExists($stateId, $updater, $storageName, $namespace);
                break;
            } catch (BulkWriteException $bulkWriteException) {
                continue;
            }
        }
    }

    private function getStateClass(callable $update)
    {
        $reflection = new ReflectionFunction($update);

        if ($reflection->getNumberOfParameters() <= 0) {
            throw new InvalidArgumentException('Updater callback must have one type-hinted parameter');
        }

        $parameter = $reflection->getParameters()[0];

        return [$parameter->getClass()->name, $parameter->isOptional()];
    }

    private function tryUpdateState($stateId, callable $updater, string $storageName, string $namespace = '')
    {
        list($stateClass, $isOptional) = $this->getStateClass($updater);

        $currentState = $this->loadStateWithVersion($stateClass, $stateId, $version, $storageName, $namespace);

        if (0 === $version && !$isOptional) {
            $currentState = new $stateClass;
        }

        $newState = call_user_func($updater, $currentState);

        $this->updateStateWithVersion($stateClass, $stateId, $newState, $version, $storageName, $namespace);
    }

    private function tryUpdateStateIfExists($stateId, callable $updater, string $storageName, string $namespace = '')
    {
        list($stateClass, $isOptional) = $this->getStateClass($updater);

        $currentState = $this->loadStateWithVersion($stateClass, $stateId, $version, $storageName, $namespace);
        if (null === $currentState) {
            return;
        }
        $newState = call_user_func($updater, $currentState);
        $this->updateStateWithVersion($stateClass, $stateId, $newState, $version, $storageName, $namespace);
    }

    private function updateStateWithVersion(string $stateClass, $stateId, $newState, int $versionWhenLoaded, string $storageName, string $namespace = '')
    {
        $this->getCollection($storageName, $namespace)->insertOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
            'payload'    => serialize($newState),
            'version'    => $versionWhenLoaded + 1,
        ]);

        $this->getCollection($storageName, $namespace)->deleteOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
            'version'    => ['$lte' => $versionWhenLoaded],
        ]);
    }

    public function debugGetVersionCountForState(string $stateClass, $stateId, string $storageName = 'global_namespace', string $namespace = ''): int
    {
        return $this->getCollection($storageName, $namespace)->count([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
        ]);
    }

    public function clearAllStates(string $storageName = 'global_namespace', string $namespace = '')
    {
        $this->getCollection($storageName, $namespace)->deleteMany([]);
    }

    private function getCollection(string $storageName, string $namespace): Collection
    {
        return $this->database->selectCollection($this->factoryCollectionName($storageName, $namespace));
    }

    private function factoryCollectionName(string $storageName, string $namespace): string
    {
        $md5 = md5($storageName);
        if (false !== strpos($storageName, '\\')) {
            $storageName = array_reverse(explode('\\', $storageName))[0];
            if (strlen($storageName) > 20) {
                $storageName = substr($storageName, 0, 20);
            }
            $name = $storageName . '_' . substr($md5, 0, 4);
        } else {
            $name = $md5;
        }

        return $namespace . '_ps_' . $name;
    }

    public function moveEntireNamespace(string $sourceNamespace, string $destinationNamespace)
    {
        $collections = $this->database->listCollections([
            'filter' => [
                'name' => new Regex('^' . preg_quote($sourceNamespace) . '_ps_.*'),
            ]
        ]);
        foreach ($collections as $collection) {
            $old = $collection->getName();
            $new = preg_replace('#^' . preg_quote($sourceNamespace) . '_ps_#ims', $destinationNamespace . '_ps_', $old);
            $this->adminDatabase->command([
                "renameCollection" => "{$this->database->getDatabaseName()}.{$old}",
                "to"               => "{$this->database->getDatabaseName()}.{$new}",
                'dropTarget'       => true,
            ]);
        }
    }

    public function deleteState($stateId, string $stateClass, string $storageName, string $namespace = '')
    {
        $this->getCollection($storageName, $namespace)->deleteMany([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
        ]);
    }
}