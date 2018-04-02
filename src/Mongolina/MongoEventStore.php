<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\EventStore;
use Dudulina\EventStore\AggregateEventStream;
use Dudulina\EventStore\EventStream;
use Dudulina\EventStore\Exception\ConcurrentModificationException;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;
use Mongolina\EventsCommit\CommitSerializer;

class MongoEventStore implements EventStore
{
    const EVENTS_EVENT_CLASS = 'events.eventClass';
    const EVENTS             = 'events';
    const EVENT_CLASS        = 'eventClass';
    const TS                 = 'ts';
    const PAYLOAD            = 'payload';
    const DUMP               = 'dump';
    const STREAM_NAME        = 'streamName';

    /** @var  Collection */
    protected $collection;
    /**
     * @var MongoAggregateAllEventStreamFactory
     */
    private $aggregateEventStreamFactory;
    /**
     * @var MongoAllEventByClassesStreamFactory
     */
    private $allEventByClassesStreamFactory;
    /**
     * @var \Mongolina\EventsCommit\CommitSerializer
     */
    private $commitSerializer;

    public function __construct(
        Collection $collection,
        MongoAggregateAllEventStreamFactory $aggregateEventStreamFactory,
        MongoAllEventByClassesStreamFactory $allEventByClassesStreamFactory,
        CommitSerializer $commitSerializer
    )
    {
        $this->collection = $collection;
        $this->aggregateEventStreamFactory = $aggregateEventStreamFactory;
        $this->allEventByClassesStreamFactory = $allEventByClassesStreamFactory;
        $this->commitSerializer = $commitSerializer;
    }

    public function loadEventsForAggregate(AggregateDescriptor $aggregateDescriptor): AggregateEventStream
    {
        return $this->aggregateEventStreamFactory->createStream($this->collection, $aggregateDescriptor);
    }

    public function createStore()
    {
        $this->collection->createIndex([self::STREAM_NAME => 1, 'version' => 1], ['unique' => true]);
        $this->collection->createIndex([self::EVENTS_EVENT_CLASS => 1, self::TS => 1]);
        $this->collection->createIndex([self::TS => 1]);
        $this->collection->createIndex(['events.id' => 1]);
    }

    public function dropStore()
    {
        $this->collection->drop();
    }

    public function appendEventsForAggregate(AggregateDescriptor $aggregateDescriptor, $eventsWithMetaData, AggregateEventStream $expectedEventStream): void
    {
        if (!$eventsWithMetaData) {
            return;
        }

        /** @var MongoAggregateAllEventStream $expectedEventStream */

        $firstEventWithMetaData = reset($eventsWithMetaData);

        try {
            $authenticatedUserId = $firstEventWithMetaData->getMetaData()->getAuthenticatedUserId();

            $this->collection->insertOne(
                $this->commitSerializer->toDocument(
                    new EventsCommit(
                        StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor),
                        (string)$aggregateDescriptor->getAggregateId(),
                        $aggregateDescriptor->getAggregateClass(),
                        1 + $expectedEventStream->getVersion(),
                        new Timestamp(0, 0),
                        new UTCDateTime(microtime(true) * 1000),
                        $authenticatedUserId ? (string)$authenticatedUserId : null,
                        $firstEventWithMetaData->getMetaData()->getCommandMetadata(),
                        $eventsWithMetaData
                    )
                )
            );
        } catch (BulkWriteException $bulkWriteException) {
            throw new ConcurrentModificationException($bulkWriteException->getMessage());
        }
    }

    public function loadEventsByClassNames(array $eventClasses): EventStream
    {
        return $this->allEventByClassesStreamFactory->createStream($this->collection, $eventClasses);
    }

    public function findEventById(string $eventId): ?EventWithMetaData
    {
        $document = $this->fetchEventDocumentById($eventId);
        return $document ? $this->commitSerializer->extractEventFromCommit($document, $eventId) : null;
    }

    public function fetchEventDocumentById(string $eventId)
    {
        $document = $this->collection->findOne([
            'events.id' => $eventId,
        ]);

        return $document;
    }

    public function getAggregateVersion(AggregateDescriptor $aggregateDescriptor)
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateDescriptor);
    }
}