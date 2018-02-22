<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
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
    /**
     * @var AggregateDescriptor
     */
    private $aggregateDescriptor;

    public function __construct(
        Collection $collection,
        AggregateDescriptor $aggregateDescriptor,
        EventStreamIterator $eventStreamIterator
    )
    {
        $this->collection = $collection;
        $this->version = $this->fetchLatestVersion($aggregateDescriptor);
        $this->sequence = $this->fetchLatestSequence();
        $this->eventStreamIterator = $eventStreamIterator;
        $this->aggregateDescriptor = $aggregateDescriptor;
    }

    public function getIterator()
    {
        return $this->eventStreamIterator->getIteratorThatExtractsEventsFromDocument(
            $this->getCursorLessThanOrEqualToVersion($this->aggregateDescriptor));
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    private function fetchLatestVersion(AggregateDescriptor $aggregateDescriptor): int
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateDescriptor);
    }

    private function fetchLatestSequence(): int
    {
        return (new LastAggregateSequenceFetcher())->fetchLatestSequence($this->collection);
    }

    private function getCursorLessThanOrEqualToVersion(AggregateDescriptor $aggregateDescriptor): Cursor
    {
        return $this->collection->find(
            [
                'streamName' => new ObjectID(StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor)),
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