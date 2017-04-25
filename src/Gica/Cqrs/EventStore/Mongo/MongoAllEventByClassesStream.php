<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Event\EventWithMetaData;
use Gica\Cqrs\EventStore\EventsCommit;
use Gica\Cqrs\EventStore\EventStreamGroupedByCommit;
use Gica\Iterator\IteratorTransformer\IteratorExpander;
use Gica\Iterator\IteratorTransformer\IteratorMapper;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;

class MongoAllEventByClassesStream implements EventStreamGroupedByCommit
{
    use EventStreamIteratorTrait;

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

    public function __construct(
        Collection $collection,
        array $eventClassNames,
        EventSerializer $eventSerializer
    )
    {
        $this->collection = $collection;
        $this->eventSerializer = $eventSerializer;
        $this->eventClassNames = $eventClassNames;
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
        $commits = $this->fetchCommits();

        return $this->extractEventsFromCommits($commits);
    }

    /**
     * @return EventsCommit[]
     */
    public function fetchCommits()
    {
        $cursor = $this->getCursor();

        /** @var EventsCommit[] $commits */
        return $this->getIteratorForCommits($cursor);
    }

    private function getCursor(): Cursor
    {
        $options = [];

        if ($this->ascending) {
            $options['sort']['sequence'] = 1;
        } else {
            $options['sort']['sequence'] = -1;
        }

        if ($this->limit > 0) {
            $options['limit'] = $this->limit;
        }

        $options['noCursorTimeout'] = true;

        $cursor = $this->collection->find(
            $this->getFilter(),
            $options
        );

        return $cursor;
    }

    /**
     * @param EventsCommit[] $commits
     * @return EventWithMetaData[]|\Generator
     */
    private function extractEventsFromCommits($commits)
    {
        $expanderCallback = function (EventsCommit $commit) {
            foreach ($commit->getEventsWithMetadata() as $eventWithMetaData) {
                if (!$this->isInterestingEvent(get_class($eventWithMetaData->getEvent()))) {
                    continue;
                }

                yield $eventWithMetaData;
            }
        };

        $generator = new IteratorExpander($expanderCallback);

        return $generator->__invoke($commits);
    }

    private function isInterestingEvent($eventClass)
    {
        return empty($this->eventClassNames) || in_array($eventClass, $this->eventClassNames);
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
            $filter['sequence'] = [
                '$gt' => $this->afterSequence,
            ];
        }

        if ($this->beforeSequence !== null) {
            $filter['sequence'] = [
                '$lt' => $this->beforeSequence,
            ];
        }

        return $filter;
    }

    private function getIteratorForCommits($cursor): \Traversable
    {
        return (new IteratorMapper(function ($document) {
            $metaData = $this->extractMetaDataFromDocument($document);

            $sequence = $this->extractSequenceFromDocument($document);
            $version = $this->extractVersionFromDocument($document);

            $events = [];

            foreach ($document['events'] as $index => $eventSubDocument) {
                if (!$this->isInterestingEvent($eventSubDocument[MongoEventStore::EVENT_CLASS])) {
                    continue;
                }

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