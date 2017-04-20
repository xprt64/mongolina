<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo\Saga;


use Gica\Cqrs\Saga\SagaEventTrackerRepository\ConcurentEventProcessingException;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;

class SagaEventTrackerRepository implements \Gica\Cqrs\Saga\SagaEventTrackerRepository
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

    public function createStorage()
    {
        $this->collection->createIndex(['sagaId' => 1, 'eventId' => 1,], ['unique' => true]);
    }

    public function isEventProcessingAlreadyStarted(string $sagaId, string $eventId): bool
    {
        return null !== $this->collection->findOne([
                'sagaId'  => $sagaId,
                'eventId' => $eventId,
            ]);
    }

    public function isEventProcessingAlreadyEnded(string $sagaId, string $eventId): bool
    {
        return null !== $this->collection->findOne([
                'sagaId'  => $sagaId,
                'eventId' => $eventId,
                'ended'   => true,
            ]);
    }

    public function startProcessingEventBySaga(string $sagaId, string $eventId)
    {
        try {
            $this->collection->insertOne([
                '_id'     => $this->factoryId(),
                'date'    => $this->factoryDate(),
                'sagaId'  => $sagaId,
                'eventId' => $eventId,
                'ended'   => false,
            ]);
        } catch (BulkWriteException $bulkWriteException) {
            throw new ConcurentEventProcessingException($bulkWriteException->getMessage());
        }
    }

    public function endProcessingEventBySaga(string $sagaId, string $eventId)
    {
        $this->collection->updateOne([
            'sagaId'  => $sagaId,
            'eventId' => $eventId,
        ], [
            '$set' => [
                'dateEnded' => $this->factoryDate(),
                'ended'     => true,
            ],
        ]);
    }

    public function clearProcessingEventBySaga(string $sagaId, string $eventId)
    {
        $this->collection->deleteOne([
            'sagaId'  => $sagaId,
            'eventId' => $eventId,
        ]);
    }

    private function factoryId(): ObjectID
    {
        return new ObjectID();
    }

    private function factoryDate(): UTCDateTime
    {
        return new UTCDateTime(microtime(true) * 1000);
    }
}