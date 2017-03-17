<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Event\EventWithMetaData;
use Gica\Cqrs\Event\ScheduledEvent;
use Gica\Cqrs\Scheduling\ScheduledEventWithMetadata;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use Gica\Types\Guid;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\UTCDateTime;

class FutureEventsStore implements \Gica\Cqrs\FutureEventsStore
{

    /* @var \MongoDB\Collection */
    private $collection;

    public function __construct(
        \MongoDB\Collection $collection
    )
    {
        $this->collection = $collection;
    }

    public function loadAndProcessScheduledEvents(callable $eventProcessor)
    {
        $scheduledEvents = $this->loadScheduledEvents();

        foreach ($scheduledEvents as $scheduledEvent) {
            $eventProcessor($scheduledEvent);

            $this->markEventAsProcessed($scheduledEvent);
        }
    }

    private function loadScheduledEvents()
    {
        $cursor = $this->collection->find([
            'scheduleAt' => [
                '$lte' => new UTCDateTime(time() * 1000),
            ],
        ], [
            'sort' => ['scheduleAt' => 1],
        ]);

        return (new IteratorMapper(function ($document) {
            return $this->extractEventWithData($document);
        }))($cursor);
    }

    /**
     * @param EventWithMetaData[] $futureEventsWithMetaData
     */
    public function scheduleEvents($futureEventsWithMetaData)
    {
        foreach ($futureEventsWithMetaData as $eventWithMetaData) {
            /** @var $event \Gica\Cqrs\Event\ScheduledEvent */
            $event = $eventWithMetaData->getEvent();
            $this->scheduleEvent($eventWithMetaData, $event->getFireDate());
        }
    }

    private function extractEventWithData($document)
    {
        return new ScheduledEventWithMetadata(
            $document['_id'],
            \unserialize($document['eventWithMetaData']));
    }

    private function markEventAsProcessed(ScheduledEventWithMetadata $scheduledEvent)
    {
        $this->collection->deleteOne([
            '_id' => new ObjectID($scheduledEvent->getEventId()),
        ]);
    }

    public function scheduleEvent(EventWithMetaData $eventWithMetaData, \DateTimeImmutable $date)
    {
        /** @var ScheduledEvent $event */
        $event = $eventWithMetaData->getEvent();

        $messageIdToMongoId = $this->messageIdToMongoId($event->getMessageId());

        $this->collection->updateOne([
            '_id'               => $messageIdToMongoId,
            'scheduleAt'        => new UTCDateTime($date->getTimestamp() * 1000),
            'eventWithMetaData' => \serialize($eventWithMetaData),
        ], [
            '$upsert' => true,
        ]);
    }

    public function createStore()
    {
        $this->collection->createIndex(['scheduleAt' => 1, 'version' => 1]);
    }

    private function messageIdToMongoId($messageId): ObjectID
    {
        return new ObjectID(Guid::fromFixedString('scheduled-event-' . $messageId));
    }
}