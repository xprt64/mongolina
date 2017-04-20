<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo\Saga\State;


use Gica\Cqrs\Saga\State\ProcessStateLoader;
use Gica\Cqrs\Saga\State\ProcessStateUpdater;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\BulkWriteException;

/**
 * State manager; uses optimistic locking
 */
class StateManager implements ProcessStateUpdater, ProcessStateLoader
{

    /**
     * @var Database
     */
    private $database;

    public function __construct(
        Database $database
    )
    {
        $this->database = $database;
    }

    public function createStorage()
    {
    }

    public function loadState(string $stateClass, $stateId, string $namespace = 'global_namespace')
    {
        return $this->loadStateWithVersion($stateClass, $stateId, $version, $namespace);
    }

    private function loadStateWithVersion(string $stateClass, $stateId, &$version, string $namespace)
    {
        $cursor = $this->getCollection($namespace)->find([
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
        } else {
            $version = 0;
            return null;
        }
    }

    public function updateState($stateId, callable $updater, string $namespace = 'global_namespace')
    {
        while (true) {
            try {
                $this->tryUpdateState($stateId, $updater, $namespace);
                break;
            } catch (BulkWriteException $bulkWriteException) {
                continue;
            }
        }
    }

    private function getStateClass(callable $update)
    {
        $reflection = new \ReflectionFunction($update);

        if ($reflection->getNumberOfParameters() <= 0) {
            throw new \Exception("Updater callback must have one type-hinted parameter");
        }

        $parameter = $reflection->getParameters()[0];

        return [$parameter->getClass()->name, $parameter->isOptional()];
    }

    private function tryUpdateState($stateId, callable $updater, string $namespace)
    {
        list($stateClass, $isOptional) = $this->getStateClass($updater);

        $currentState = $this->loadStateWithVersion($stateClass, $stateId, $version, $namespace);

        if (0 === $version) {
            if (!$isOptional) {
                $currentState = new $stateClass;
            }
        }

        $newState = call_user_func($updater, $currentState);

        $this->updateStateWithVersion($stateClass, $stateId, $newState, $version, $namespace);
    }

    private function updateStateWithVersion(string $stateClass, $stateId, $newState, int $versionWhenLoaded, string $namespace)
    {
        $this->getCollection($namespace)->insertOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
            'payload'    => serialize($newState),
            'version'    => $versionWhenLoaded + 1,
        ]);

        $this->getCollection($namespace)->deleteOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
            'version'    => ['$lte' => $versionWhenLoaded],
        ]);
    }

    public function debugGetVersionCountForState(string $stateClass, $stateId, string $namespace = 'global_namespace'): int
    {
        return $this->getCollection($namespace)->count([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
        ]);
    }

    public function clearAllStates(string $namespace = 'global_namespace')
    {
        $this->getCollection($namespace)->deleteMany([]);
    }

    private function getCollection(string $namespace): Collection
    {
        $collection = $this->database->selectCollection(preg_replace('#[^a-zA-Z0-9_]#ims', '_', $namespace));

        $collection->createIndex(['stateClass' => 1, 'stateId' => 1,]);
        $collection->createIndex(['stateClass' => 1, 'stateId' => 1, 'version' => -1], ['unique' => true]);

        return $collection;
    }
}