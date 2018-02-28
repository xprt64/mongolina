<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\EventStore\EventStream;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use MongoDB\BSON\Timestamp;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use Mongolina\EventsCommit\CommitSerializer;

class MongoAllEventByClassesStream implements EventStream
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

    /** @var Timestamp|null */
    private $afterTimestamp;

    /** @var Timestamp|null */
    private $beforeTimestamp;

    private $ascending = true;
    /**
     * @var \Mongolina\EventsCommit\CommitSerializer
     */
    private $commitSerializer;

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

    public function afterTimestamp(Timestamp $timestamp)
    {
        $this->afterTimestamp = $timestamp;
        $this->ascending = true;
    }

    public function beforeTimestamp(Timestamp $timestamp)
    {
        $this->beforeTimestamp = $timestamp;
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

    private function getCursorForEvents(): \Traversable
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

        if ($this->afterTimestamp !== null) {
            $filter[MongoEventStore::TS] = [
                '$gt' => $this->afterTimestamp,
            ];
        }

        if ($this->beforeTimestamp !== null) {
            $filter[MongoEventStore::TS] = [
                '$lt' => $this->beforeTimestamp,
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
        return (new IteratorMapper(function ($document) {
            return $this->commitSerializer->extractEventFromSubDocument($document['events'], $document);
        }))($documents);
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
            '$unwind' => '$events',
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