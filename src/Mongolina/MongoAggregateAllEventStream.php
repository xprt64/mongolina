<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\EventStore\AggregateEventStream;
use MongoDB\BSON\ObjectID;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;

class MongoAggregateAllEventStream implements AggregateEventStream
{
    /**
     * @var Collection
     */
    private $collection;
    private $aggregateId;

    /** @var int */
    private $version;
    /**
     * @var string
     */
    private $aggregateClass;

    /** @var  int */
    private $sequence;
    /**
     * @var EventStreamIterator
     */
    private $eventStreamIterator;

    public function __construct(
        Collection $collection,
        string $aggregateClass,
        $aggregateId,
        EventStreamIterator $eventStreamIterator
    )
    {
        $this->collection = $collection;
        $this->aggregateClass = $aggregateClass;
        $this->aggregateId = $aggregateId;
        $this->version = $this->fetchLatestVersion($aggregateClass, $aggregateId);
        $this->sequence = $this->fetchLatestSequence();
        $this->eventStreamIterator = $eventStreamIterator;
    }

    public function getIterator()
    {
        return $this->eventStreamIterator->getIteratorThatExtractsEventsFromDocument(
            $this->getCursorLessThanOrEqualToVersion($this->aggregateClass, $this->aggregateId));
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    private function fetchLatestVersion(string $aggregateClass, $aggregateId): int
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateClass, $aggregateId);
    }

    private function fetchLatestSequence(): int
    {
        return (new LastAggregateSequenceFetcher())->fetchLatestSequence($this->collection);
    }

    private function getCursorLessThanOrEqualToVersion(string $aggregateClass, $aggregateId): Cursor
    {
        return $this->collection->find(
            [
                'streamName' => new ObjectID(StreamName::factoryStreamName($aggregateClass, $aggregateId)),
                'version'    => [
                    '$lte' => $this->version,
                ],
            ],
            [
                'sort' => [
                    'sequence' => 1,
                ],
            ]
        );
    }

    public function getSequence(): int
    {
        return $this->sequence;
    }
}