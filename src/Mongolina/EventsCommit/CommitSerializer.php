<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\EventsCommit;


use Dudulina\Event\EventWithMetaData;
use Dudulina\Event\MetaData;
use Gica\Serialize\ObjectHydrator\ObjectHydrator;
use Gica\Serialize\ObjectSerializer\ObjectSerializer;
use Gica\Types\Guid;
use Mongolina\EventsCommit;
use Mongolina\EventSequence;
use Mongolina\EventSerializer;
use Mongolina\MongoEventStore;

class CommitSerializer
{
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var ObjectSerializer
     */
    private $objectSerializer;
    /**
     * @var ObjectHydrator
     */
    private $objectHydrator;

    public function __construct(
        EventSerializer $eventSerializer,
        ObjectSerializer $objectSerializer,
        ObjectHydrator $objectHydrator
    ) {
        $this->objectHydrator = $objectHydrator;
        $this->eventSerializer = $eventSerializer;
        $this->objectSerializer = $objectSerializer;
    }

    public function fromDocument($document): EventsCommit
    {
        return new EventsCommit(
            $document['streamName'],
            $document['aggregateId'],
            $document['aggregateClass'],
            $document['version'],
            $document['ts'],
            $document['createdAt'],
            $document['authenticatedUserId'],
            $document['commandMeta'] ? $this->commandMetaFromDocument($document['commandMeta']) : null,
            $this->unpackEvents($document)
        );
    }

    private function commandMetaFromDocument($document)
    {
        return json_decode($document, true);
    }

    private function unpackEvents($document)
    {
        return array_map(function ($index, $eventSubDocument) use ($document) {
            return $this->extractEventFromSubDocument($eventSubDocument, $index, $document);
        }, array_keys($document['events']), $document['events']);
    }

    public function extractEventFromSubDocument($eventSubDocument, $index, $document): EventWithMetaData
    {
        $metaData = new MetaData(
            (string)$document['aggregateId'],
            $document['aggregateClass'],
            \DateTimeImmutable::createFromMutable($document['createdAt']->toDateTime()),
            $document['authenticatedUserId']
        );

        return new EventWithMetaData(
            $this->unserializeEvent($eventSubDocument),
            $metaData
                ->withEventId($eventSubDocument['id'])
                ->withVersion($document['version'])
                ->withSequence(new EventSequence($document['ts'], $index))
        );
    }

    private function unserializeEvent($eventSubDocument)
    {
        return $this->objectHydrator->hydrateObject($eventSubDocument[MongoEventStore::EVENT_CLASS], $eventSubDocument['dump']);
    }

    public function toDocument(EventsCommit $commit): array
    {
        return [
            'streamName'          => $commit->getStreamName(),
            'aggregateId'         => $commit->getAggregateId(),
            'aggregateClass'      => $commit->getAggregateClass(),
            'version'             => $commit->getVersion(),
            'ts'                  => $commit->getTs(),
            'createdAt'           => $commit->getCreatedAt(),
            'authenticatedUserId' => $commit->getAuthenticatedUserId(),
            'commandMeta'         => json_encode($commit->getCommandMeta()),
            'events'              => $this->packEvents($commit->getEventsWithMetadata()),
        ];
    }

    private function packEvents($events): array
    {
        return array_map([$this, 'packEvent'], $events);
    }

    public function extractEventFromCommit($document, string $eventId): ?EventWithMetaData
    {
        foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubDocument) {
            if ($eventSubDocument['id'] !== $eventId) {
                continue;
            }

            return $this->extractEventFromSubDocument($eventSubDocument, $index, $document);
        }

        return null;
    }

    public function extractEventFromCommitHydrate($document, string $eventId): ?EventWithMetaData
    {
        foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubDocument) {
            if ($eventSubDocument['id'] !== $eventId) {
                continue;
            }

            return $this->extractEventFromSubDocumentHydrate($eventSubDocument, $index, $document);
        }

        return null;
    }

    public function packEvent(EventWithMetaData $eventWithMetaData): array
    {
        return [
            MongoEventStore::EVENT_CLASS => \get_class($eventWithMetaData->getEvent()),
            MongoEventStore::PAYLOAD     => $this->eventSerializer->serializeEvent($eventWithMetaData->getEvent()),
            'dump'                       => $this->objectSerializer->convert($eventWithMetaData->getEvent()),
            'id'                         => $eventWithMetaData->getMetaData()->getEventId(),
        ];
    }

    public function copyEvent($from, $to)
    {
        $to[MongoEventStore::EVENT_CLASS] = $from[MongoEventStore::EVENT_CLASS];
        $to[MongoEventStore::PAYLOAD] = $from[MongoEventStore::PAYLOAD];
        $to['dump'] = $from['dump'];
        $to['id'] = $from['id'];

        return $to;
    }
}