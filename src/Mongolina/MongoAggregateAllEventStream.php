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
    use EventStreamIteratorTrait;

    /**
     * @var Collection
     */
    private $collection;
    private $aggregateId;
    private $version;
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var string
     */
    private $aggregateClass;

    /** @var  int */
    private $sequence;

    public function __construct(
        Collection $collection,
        string $aggregateClass,
        $aggregateId,
        EventSerializer $eventSerializer
    )
    {
        $this->collection = $collection;
        $this->aggregateClass = $aggregateClass;
        $this->aggregateId = $aggregateId;
        $this->eventSerializer = $eventSerializer;
        $this->version = $this->fetchLatestVersion($aggregateClass, $aggregateId);
        $this->sequence = $this->fetchLatestSequence();
    }

    /**
     * @inheritdoc
     */
    public function getIterator()
    {
        $cursor = $this->getCursorLessThanOrEqualToVersion($this->aggregateClass, $this->aggregateId);

        return $this->getIteratorThatExtractsEventsFromDocument($cursor);
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
        $cursor = $this->collection->find(
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
        return $cursor;
    }

    public function getSequence(): int
    {
        return $this->sequence;
    }
}