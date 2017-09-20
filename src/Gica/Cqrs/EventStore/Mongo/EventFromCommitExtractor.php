<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Event\EventWithMetaData;

class EventFromCommitExtractor
{
    use DocumentParserTrait;

    /**
     * @var EventSerializer
     */
    private $eventSerializer;

    public function __construct(
        EventSerializer $eventSerializer
    )
    {
        $this->eventSerializer = $eventSerializer;
    }

    public function extractEventFromCommit(array $document, string $eventId): ?EventWithMetaData
    {
        $metaData = $this->extractMetaDataFromDocument($document);

        $sequence = $this->extractSequenceFromDocument($document);

        foreach ($document['events'] as $index => $eventSubDocument) {
            if ($eventSubDocument['id'] !== $eventId) {
                continue;
            }

            $event = $this->eventSerializer->deserializeEvent($eventSubDocument[MongoEventStore::EVENT_CLASS], $eventSubDocument['payload']);

            $eventWithMetaData = new EventWithMetaData($event, $metaData->withEventId($eventSubDocument['id']));

            return $eventWithMetaData->withSequenceAndIndex($sequence, $index);
        }

        return null;
    }
}