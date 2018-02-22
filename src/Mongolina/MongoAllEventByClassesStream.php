<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Event\EventWithMetaData;
use Dudulina\EventStore\EventsCommit;
use Dudulina\EventStore\EventStreamGroupedByCommit;
use Gica\Iterator\IteratorTransformer\IteratorExpander;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;

class MongoAllEventByClassesStream implements EventStreamGroupedByCommit
{

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var array
     */
    private $eventClassNames;

    /** @var int|null */
    private $limit = null;

    /** @var int|null */
    private $afterSequence;

    /** @var int|null */
    private $beforeSequence;

    private $ascending = true;
    /**
     * @var DocumentParser
     */
    private $documentParser;

    public function __construct(
        Collection $collection,
        array $eventClassNames,
        EventSerializer $eventSerializer,
        DocumentParser $documentParser
    )
    {
        $this->collection = $collection;
        $this->eventSerializer = $eventSerializer;
        $this->eventClassNames = $eventClassNames;
        $this->documentParser = $documentParser;
    }

    /**
     * @inheritdoc
     */
    public function limitCommits(int $limit)
    {
        $this->limit = $limit;
    }

    /**
     * @inheritdoc
     */
    public function afterSequence(int $afterSequence)
    {
        $this->afterSequence = $afterSequence;
        $this->ascending = true;
    }

    /**
     * @inheritdoc
     */
    public function beforeSequence(int $beforeSequence)
    {
        $this->beforeSequence = $beforeSequence;
        $this->ascending = false;
    }

    public function countCommits(): int
    {
        return $this->collection->count($this->getFilter());
    }

    /**
     * @inheritdoc
     */
    public function getIterator()
    {
        return $this->extractEventsFromCommits($this->fetchCommits());
    }

    public function fetchCommits()
    {
        return $this->getIteratorForCommits($this->getCursor());
    }

    private function getCursor(): Cursor
    {
        $options = [
            'sort'            => [
                MongoEventStore::SEQUENCE => $this->ascending ? 1 : -1,
            ],
            'noCursorTimeout' => true,
        ];

        if ($this->limit > 0) {
            $options['limit'] = $this->limit;
        }

        return $this->collection->find(
            $this->getFilter(),
            $options
        );
    }

    /**
     * @param EventsCommit[] $commits
     * @return EventWithMetaData[]|\Iterator
     */
    private function extractEventsFromCommits($commits)
    {
        return (new IteratorExpander(function (EventsCommit $commit) {
            foreach ($commit->getEventsWithMetadata() as $eventWithMetaData) {
                yield $eventWithMetaData;
            }
        }))->__invoke($commits);
    }

    private function getFilter(): array
    {
        $filter = [];

        if ($this->eventClassNames) {
            $filter[MongoEventStore::EVENTS_EVENT_CLASS] = [
                '$in' => $this->eventClassNames,
            ];
        }

        if ($this->afterSequence !== null) {
            $filter[MongoEventStore::SEQUENCE] = [
                '$gt' => $this->afterSequence,
            ];
        }

        if ($this->beforeSequence !== null) {
            $filter[MongoEventStore::SEQUENCE] = [
                '$lt' => $this->beforeSequence,
            ];
        }

        return $filter;
    }

    private function getIteratorForCommits($cursor): \Traversable
    {
        return (new IteratorMapper(function ($document) {
            $metaData = $this->documentParser->extractMetaDataFromDocument($document);
            $sequence = $this->documentParser->extractSequenceFromDocument($document);
            $version = $this->documentParser->extractVersionFromDocument($document);

            $events = [];

            foreach ($document['events'] as $index => $eventSubDocument) {
                $event = $this->eventSerializer->deserializeEvent($eventSubDocument[MongoEventStore::EVENT_CLASS], $eventSubDocument['payload']);

                $eventWithMetaData = new EventWithMetaData($event, $metaData->withEventId($eventSubDocument['id']));

                $events[] = $eventWithMetaData->withSequenceAndIndex($sequence, $index);
            }

            return new EventsCommit(
                $sequence,
                $version,
                $events
            );
        }))($cursor);
    }
}