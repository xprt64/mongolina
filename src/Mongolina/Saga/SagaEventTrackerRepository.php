<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\Saga;


use Dudulina\EventProcessing\ConcurentEventProcessingException;
use Dudulina\EventProcessing\InProgressProcessingEvent;
use Mongolina\EventProcessing\MongoInProgressProcessingEvent;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use Gica\MongoDB\Selector\Filter\Comparison\EqualDirect;
use Gica\MongoDB\Selector\Selector;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;

class SagaEventTrackerRepository implements \Dudulina\Saga\SagaEventTrackerRepository
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
        $this->collection->createIndex(['sagaId' => 1, 'ended' => 1,]);
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

    public function startProcessingEvent(string $sagaId, string $eventId)
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

    public function endProcessingEvent(string $sagaId, string $eventId)
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

    public function clearProcessingEvent(string $sagaId, string $eventId)
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

    /**
     * @param string $processId
     * @return InProgressProcessingEvent[]|\Countable|\Iterator
     */
    public function getAllInProgressProcessingEvents(string $processId)
    {
        return (new Selector($this->collection))
            ->setIteratorMapper(new IteratorMapper(function ($document) {
                /** @var UTCDateTime $date */
                $date = $document['date'];

                return new MongoInProgressProcessingEvent(
                    \DateTimeImmutable::createFromMutable($date->toDateTime()),
                    $document['eventId']
                );
            }))
            ->sort('date', true)
            ->addFilter(new EqualDirect('ended', false))
            ->addFilter(new EqualDirect('sagaId', $processId));
    }

    public function resetTracker(string $sagaId)
    {
        $this->collection->deleteMany([
            'sagaId'  => $sagaId,
        ]);
    }
}