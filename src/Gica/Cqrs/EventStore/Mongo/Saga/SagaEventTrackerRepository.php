<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo\Saga;


use Gica\Cqrs\Saga\EventOrder;
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
    private $collectionForStarted;
    /**
     * @var Collection
     */
    private $collectionForEnded;

    public function __construct(
        Collection $collectionForStarted,
        Collection $collectionForEnded
    )
    {
        $this->collectionForStarted = $collectionForStarted;
        $this->collectionForEnded = $collectionForEnded;
    }

    public function createStorage()
    {
        $this->collectionForStarted->createIndex(['sagaId' => 1, 'sequence' => -1, 'index' => -1], ['unique' => true]);
        $this->collectionForEnded->createIndex(['sagaId' => 1, 'sequence' => -1, 'index' => -1], ['unique' => true]);
    }

    public function isEventProcessingAlreadyStarted(string $sagaId, EventOrder $eventOrder): bool
    {
        return null !== $this->collectionForStarted->findOne([
                'sagaId'   => $sagaId,
                'sequence' => $eventOrder->getSequence(),
                'index'    => $eventOrder->getIndex(),
            ]);
    }

    public function isEventProcessingAlreadyEnded(string $sagaId, EventOrder $eventOrder): bool
    {
        return null !== $this->collectionForEnded->findOne([
                'sagaId'   => $sagaId,
                'sequence' => $eventOrder->getSequence(),
                'index'    => $eventOrder->getIndex(),
            ]);
    }

    public function startProcessingEventBySaga(string $sagaId, EventOrder $eventOrder)
    {
        try {
            $this->collectionForStarted->insertOne([
                '_id'      => $this->factoryId(),
                'date'     => $this->factoryDate(),
                'sagaId'   => $sagaId,
                'sequence' => $eventOrder->getSequence(),
                'index'    => $eventOrder->getIndex(),
            ]);
        } catch (BulkWriteException $bulkWriteException) {
            throw new ConcurentEventProcessingException($bulkWriteException->getMessage());
        }
    }

    public function endProcessingEventBySaga(string $sagaId, EventOrder $eventOrder)
    {
        try {
            $this->collectionForEnded->insertOne([
                '_id'      => $this->factoryId(),
                'date'     => $this->factoryDate(),
                'sagaId'   => $sagaId,
                'sequence' => $eventOrder->getSequence(),
                'index'    => $eventOrder->getIndex(),
            ]);
        } catch (BulkWriteException $bulkWriteException) {
            throw new ConcurentEventProcessingException($bulkWriteException->getMessage());
        }
    }

    public function getLastStartedEventSequenceAndIndex(string $sagaId):?EventOrder
    {
        $cursor = $this->collectionForStarted->find([
            'sagaId' => $sagaId,
        ], [
            'projection' => [
                'sequence' => 1,
                'index'    => 1,
            ],
            'sort'       => [
                'sequence' => -1,
                'index'    => -1,
            ],
            'limit'      => 1,
        ]);

        $documents = iterator_to_array($cursor);

        if ($documents) {
            return new EventOrder((int)$documents[0]['sequence'], (int)$documents[0]['index']);
        }

        return null;
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