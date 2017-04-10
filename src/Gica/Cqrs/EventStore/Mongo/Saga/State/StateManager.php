<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo\Saga\State;


use Gica\Cqrs\Saga\State\ProcessStateLoader;
use Gica\Cqrs\Saga\State\ProcessStateUpdater;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;

class StateManager implements ProcessStateUpdater, ProcessStateLoader
{

    /**
     * @var Collection
     */
    private $collection;

    public function __construct(
        Collection $collection
    )
    {
        $this->collection = $collection;
    }

    public function createCollection()
    {
        $this->collection->createIndex(['stateClass' => 1, 'stateId' => 1, 'version' => -1], ['unique' => true]);
    }

    public function loadState(string $stateClass, $stateId)
    {
        return $this->loadStateWithVersion($stateClass, $stateId, $version);
    }

    public function updateState($stateId, callable $updater)
    {
        $stateClass = $this->getStateClass($updater);

        while (true) {
            try {
                $this->tryUpdateState($stateClass, $stateId, $updater);
                break;
            } catch (BulkWriteException $bulkWriteException) {
                continue;
            }
        }
    }

    private function tryUpdateState(string $stateClass, $stateId, callable $updater)
    {
        $currentState = $this->loadStateWithVersion($stateClass, $stateId, $version);

        $newState = call_user_func($updater, $currentState);

        $this->updateStateWithVersion($stateClass, $stateId, $newState, $version);
    }


    private function loadStateWithVersion(string $stateClass, $stateId, &$version)
    {
        $cursor = $this->collection->find([
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
            return new $stateClass;
        }
    }

    private function getStateClass(callable $update): string
    {
        $reflection = new \ReflectionFunction($update);

        if ($reflection->getNumberOfParameters() <= 0) {
            throw new \Exception("Updater callback must have one type-hinted parameter");
        }

        return $reflection->getParameters()[0]->getClass()->name;
    }

    private function updateStateWithVersion(string $stateClass, $stateId, $newState, int $versionWhenLoaded)
    {
        $this->collection->updateOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
        ], [
            '$set' => [
                'payload' => serialize($newState),
                'version' => $versionWhenLoaded + 1,
            ],
        ], [
            'upsert' => true,
        ]);

        $this->collection->deleteOne([
            'stateClass' => $stateClass,
            'stateId'    => (string)$stateId,
            'version'    => $versionWhenLoaded,
        ]);
    }
}