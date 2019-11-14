<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Event\EventWithMetaData;
use Dudulina\EventStore\SeekableEventStream;
use Gica\Iterator\IteratorTransformer\IteratorFilter;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use Mongolina\EventsCommit\CommitSerializer;

class MongoAllEventByClassesStream implements SeekableEventStream
{

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var array
     */
    private $eventClassNames;

    /** @var int|null */
    private $limit = null;

    private $ascending = true;
    /**
     * @var \Mongolina\EventsCommit\CommitSerializer
     */
    private $commitSerializer;

    /** @var EventSequence|null */
    private $afterSequence;

    /** @var EventSequence|null */
    private $beforeSequence;

    public function __construct(
        Collection $collection,
        array $eventClassNames,
        CommitSerializer $commitSerializer
    )
    {
        $this->collection = $collection;
        $this->eventClassNames = $eventClassNames;
        $this->commitSerializer = $commitSerializer;
    }

    /**
     * @inheritdoc
     */
    public function limitCommits(int $limit)
    {
        $this->limit = $limit;
    }

    public function afterSequence(\Dudulina\EventStore\EventSequence $after)
    {
        $this->afterSequence = $after;
    }

    public function beforeSequence(\Dudulina\EventStore\EventSequence $before)
    {
        $this->beforeSequence = $before;
    }

    public function sort(bool $chronological)
    {
        $this->ascending = $chronological;
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
        return $this->getIteratorForEvents($this->getCursorForEvents());
    }

    /**
     * @return \Traversable|EventsCommit[]
     */
    public function fetchCommits()
    {
        return $this->getIteratorForCommits($this->getCursorForCommits());
    }

    private function getCursorForCommits(): Cursor
    {
        $options = [
            'sort'            => [
                MongoEventStore::TS => $this->ascending ? 1 : -1,
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

    public function getCursorForEvents(): \Traversable
    {
        $pipeline = $this->getEventsPipeline(true);

        $options = [
            'noCursorTimeout' => true,
        ];

        return $this->collection->aggregate(
            $pipeline,
            $options
        );
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
            $filter[MongoEventStore::TS] = [
                '$gt' => $this->afterSequence->getTimestamp(),
            ];
        }

        if ($this->beforeSequence !== null) {
            $filter[MongoEventStore::TS] = [
                '$lt' => $this->beforeSequence->getTimestamp(),
            ];
        }

        return $filter;
    }

    private function getIteratorForCommits($cursor): \Traversable
    {
        return (new IteratorMapper(function ($document) {
            return $this->commitSerializer->fromDocument($document);
        }))($cursor);
    }

    private function getIteratorForEvents($documents): \Traversable
    {
        $events = (new IteratorMapper(function ($document) {
            return $this->commitSerializer->extractEventFromSubDocument($document['events'], $document['index'], $document);
        }))($documents);

        if ($this->afterSequence) {
            $filter = new IteratorFilter(function (EventWithMetaData $eventWithMetaData) {
                /** @var EventSequence $eventSequence */
                $eventSequence = $eventWithMetaData->getMetaData()->getSequence();
                return $eventSequence->isAfter($this->afterSequence);
            });
            $events = $filter($events);
        }

        if ($this->beforeSequence) {
            $filter = new IteratorFilter(function (EventWithMetaData $eventWithMetaData) {
                /** @var EventSequence $eventSequence */
                $eventSequence = $eventWithMetaData->getMetaData()->getSequence();
                return $eventSequence->isBefore($this->beforeSequence);
            });
            $events = $filter($events);
        }

        return $events;
    }

    private function getEventsPipeline(bool $sorted = true): array
    {
        $pipeline = [];

        if ($this->getFilter()) {
            $pipeline[] = [
                '$match' => $this->getFilter(),
            ];
        }

        if ($sorted) {
            $pipeline[] = [
                '$sort' => [
                    MongoEventStore::TS => $this->ascending ? 1 : -1,
                ],
            ];
        }

        $pipeline[] = [
            '$unwind' => [
                'path'              => '$events',
                'includeArrayIndex' => 'index',
            ],
        ];

        if ($this->getFilter()) {
            $pipeline[] = [
                '$match' => $this->getFilter(),
            ];
        }

        if ($this->limit > 0) {
            $pipeline[] = [
                '$limit' => $this->limit,
            ];
        }
        return $pipeline;
    }

    public function count()
    {
        $pipeline = $this->getEventsPipeline(false);

        $pipeline[] = [
            '$count' => 'total',
        ];

        $options = [
            'noCursorTimeout' => true,
        ];

        return iterator_to_array($this->collection->aggregate(
            $pipeline,
            $options
        ))[0]['total'];
    }

}