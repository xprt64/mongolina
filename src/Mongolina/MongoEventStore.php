<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\EventStore;
use Dudulina\EventStore\AggregateEventStream;
use Dudulina\EventStore\EventStreamGroupedByCommit;
use Dudulina\EventStore\Exception\ConcurrentModificationException;
use Gica\Lib\ObjectToArrayConverter;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;

class MongoEventStore implements EventStore
{
    const EVENTS_EVENT_CLASS = 'events.eventClass';
    const EVENT_CLASS        = 'eventClass';
    const SEQUENCE           = 'sequence';
    const TS                 = 'ts';

    /** @var  Collection */
    protected $collection;
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var ObjectToArrayConverter
     */
    private $objectToArrayConverter;
    /**
     * @var EventFromCommitExtractor
     */
    private $eventFromCommitExtractor;
    /**
     * @var MongoAggregateAllEventStreamFactory
     */
    private $aggregateEventStreamFactory;
    /**
     * @var MongoAllEventByClassesStreamFactory
     */
    private $allEventByClassesStreamFactory;

    public function __construct(
        Collection $collection,
        EventSerializer $eventSerializer,
        ObjectToArrayConverter $objectToArrayConverter,
        EventFromCommitExtractor $eventFromCommitExtractor,
        MongoAggregateAllEventStreamFactory $aggregateEventStreamFactory,
        MongoAllEventByClassesStreamFactory $allEventByClassesStreamFactory
    )
    {
        $this->collection = $collection;
        $this->eventSerializer = $eventSerializer;
        $this->objectToArrayConverter = $objectToArrayConverter;
        $this->eventFromCommitExtractor = $eventFromCommitExtractor;
        $this->aggregateEventStreamFactory = $aggregateEventStreamFactory;
        $this->allEventByClassesStreamFactory = $allEventByClassesStreamFactory;
    }

    public function loadEventsForAggregate(AggregateDescriptor $aggregateDescriptor): AggregateEventStream
    {
        return $this->aggregateEventStreamFactory->createStream($this->collection, $aggregateDescriptor);
    }

    public function createStore()
    {
        $this->collection->createIndex(['streamName' => 1, 'version' => 1], ['unique' => true]);
        $this->collection->createIndex([self::EVENTS_EVENT_CLASS => 1, self::SEQUENCE => 1]);
        $this->collection->createIndex([self::SEQUENCE => 1]);
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

        $firstEventWithMetaData = reset($eventsWithMetaData);

        try {
            $authenticatedUserId = $firstEventWithMetaData->getMetaData()->getAuthenticatedUserId();
            $this->collection->insertOne([
                'streamName'          => new ObjectID($this->factoryStreamName($aggregateDescriptor->getAggregateClass(), $aggregateDescriptor->getAggregateId())),
                'aggregateId'         => (string)$aggregateDescriptor->getAggregateId(),
                'aggregateClass'      => $aggregateDescriptor->getAggregateClass(),
                'version'             => 1 + $expectedEventStream->getVersion(),
                'ts'                  => new Timestamp(0, 0),
                self::SEQUENCE        => 1 + $expectedEventStream->getSequence(),
                'createdAt'           => new UTCDateTime(microtime(true) * 1000),
                'authenticatedUserId' => $authenticatedUserId ? (string)$authenticatedUserId : null,
                'commandMeta'         => $this->objectToArrayConverter->convert($firstEventWithMetaData->getMetaData()->getCommandMetadata()),
                'events'              => $this->packEvents($eventsWithMetaData),
            ]);
        } catch (BulkWriteException $bulkWriteException) {
            throw new ConcurrentModificationException($bulkWriteException->getMessage());
        }
    }

    private function packEvents($events): array
    {
        return array_map([$this, 'packEvent'], $events);
    }

    private function packEvent(EventWithMetaData $eventWithMetaData): array
    {
        return array_merge([
            self::EVENT_CLASS => \get_class($eventWithMetaData->getEvent()),
            'payload'         => $this->eventSerializer->serializeEvent($eventWithMetaData->getEvent()),
            'dump'            => $this->objectToArrayConverter->convert($eventWithMetaData->getEvent()),
            'id'              => $eventWithMetaData->getMetaData()->getEventId(),
        ]);
    }

    public function loadEventsByClassNames(array $eventClasses): EventStreamGroupedByCommit
    {
        return $this->allEventByClassesStreamFactory->createStream($this->collection, $eventClasses);
    }

    public function findEventById(string $eventId): ?EventWithMetaData
    {
        $document = $this->collection->findOne([
            'events.id' => $eventId,
        ]);

        return $document ? $this->eventFromCommitExtractor->extractEventFromCommit($document, $eventId) : null;
    }

    public function getAggregateVersion(AggregateDescriptor $aggregateDescriptor)
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateDescriptor);
    }

    public function fetchLatestSequence(): int
    {
        return (new LastAggregateSequenceFetcher())->fetchLatestSequence($this->collection);
    }

    public function factoryStreamName(string $aggregateClass, $aggregateId)
    {
        return StreamName::factoryStreamName($aggregateClass, $aggregateId);
    }

}