<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\Event\EventWithMetaData;

class EventFromCommitExtractor
{
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var DocumentParser
     */
    private $documentParser;

    public function __construct(
        EventSerializer $eventSerializer,
        DocumentParser $documentParser
    )
    {
        $this->eventSerializer = $eventSerializer;
        $this->documentParser = $documentParser;
    }

    public function extractEventFromCommit(array $document, string $eventId): ?EventWithMetaData
    {
        $metaData = $this->documentParser->extractMetaDataFromDocument($document);
        $sequence = $this->documentParser->extractSequenceFromDocument($document);

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