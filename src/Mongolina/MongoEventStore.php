<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\EventStore;
use Dudulina\EventStore\AggregateEventStream;
use Dudulina\EventStore\Exception\ConcurrentModificationException;
use Dudulina\EventStore\SeekableEventStream;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use Mongolina\EventsCommit\CommitSerializer;

class MongoEventStore implements EventStore
{
    public const EVENTS_EVENT_CLASS = 'events.eventClass';
    public const EVENTS             = 'events';
    public const EVENT_CLASS        = 'eventClass';
    public const TS                 = 'ts';
    public const PAYLOAD            = 'payload';
    public const DUMP               = 'dump';
    public const STREAM_NAME        = 'streamName';

    public const ANNOTATION_FOR_COMPACT_EVENTS = '@compact';

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

    public function createStore(): void
    {
        $this->collection->createIndex([self::STREAM_NAME => 1, 'version' => 1], ['unique' => true]);
        $this->collection->createIndex([self::EVENTS_EVENT_CLASS => 1, self::TS => 1]);
        $this->collection->createIndex([self::TS => 1]);
        $this->collection->createIndex(['events.id' => 1]);
    }

    public function dropStore(): void
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
        $this->compactTheStream(StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor), $expectedEventStream->getVersion(), $eventsWithMetaData);
    }

    /**
     * @param EventWithMetaData[] $events
     * @return string[]
     */
    private function getCompactableEventClasses($events): array
    {
        $onlyCompactable = array_filter($events, function (EventWithMetaData $eventWithMetaData) {
            $class = new \ReflectionClass($eventWithMetaData->getEvent());
            return false !== stripos((string)$class->getDocComment(), self::ANNOTATION_FOR_COMPACT_EVENTS);
        });
        return array_values(array_map(function (EventWithMetaData $eventWithMetaData) {
            return \get_class($eventWithMetaData->getEvent());
        }, $onlyCompactable));
    }

    private function compactTheStream(ObjectId $streamName, int $beforeAndVersion, array $eventsWithMeta): void
    {
        $eventsToBeDeleted = $this->getCompactableEventClasses($eventsWithMeta);
        if (!$eventsToBeDeleted) {
            return;
        }
        $this->collection->updateMany(
            [
                'streamName' => $streamName,
                'version'    => ['$lte' => $beforeAndVersion],
            ],
            [
                '$pull' => [
                    'events' => [
                        'eventClass' => ['$in' => $eventsToBeDeleted],
                    ],
                ],

            ]
        );
        $this->collection->deleteMany([
                'streamName' => $streamName,
                'version'    => ['$lte' => $beforeAndVersion],
                'events'     => [],
            ]
        );
    }

    public function loadEventsByClassNames(array $eventClasses): SeekableEventStream
    {
        return $this->allEventByClassesStreamFactory->createStream($this->collection, $eventClasses);
    }

    public function findEventById(string $eventId): ?EventWithMetaData
    {
        $document = $this->fetchCommitDocumentByEventById($eventId);
        return $document ? $this->commitSerializer->extractEventFromCommit($document, $eventId) : null;
    }

    public function fetchCommitDocumentByEventById(string $eventId)
    {
        $document = $this->collection->findOne([
            'events.id' => $eventId,
        ]);

        return $document;
    }

    public function fetchEventSubDocumentById(string $eventId)
    {
        $document = $this->fetchCommitDocumentByEventById($eventId);

        foreach ($document[self::EVENTS] as $eventSubDocument) {
            if ($eventSubDocument['id'] !== $eventId) {
                continue;
            }
            $document[self::EVENTS] = $eventSubDocument;
            return $document;
        }
        return null;
    }

    public function getAggregateVersion(AggregateDescriptor $aggregateDescriptor)
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateDescriptor);
    }

    public function replaceEvent($eventId, callable $updater)
    {
        $document = $this->collection->findOne(['events.id' => $eventId]);
        if (!$document) {
            return;
        }
        foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubDocument) {
            if ($eventSubDocument['id'] !== $eventId) {
                continue;
            }
            $document[MongoEventStore::EVENTS][$index] = $updater($eventSubDocument);
            break;
        }
        $this->collection->replaceOne(['events.id' => $eventId], $document);
    }

    public function deleteEvent($eventId)
    {
        $document = $this->collection->findOne(['events.id' => $eventId]);
        if (!$document) {
            return;
        }
        if (count($document[MongoEventStore::EVENTS]) === 1) {
            $this->collection->deleteOne(['events.id' => $eventId]);
        } else {
            foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubDocument) {
                if ($eventSubDocument['id'] !== $eventId) {
                    continue;
                }
                unset($document[MongoEventStore::EVENTS][$index]);
                break;
            }
            $document[MongoEventStore::EVENTS] = array_values($document[MongoEventStore::EVENTS]);
            $this->collection->replaceOne(['events.id' => $eventId], $document);
        }
    }
}