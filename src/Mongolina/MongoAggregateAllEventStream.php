<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\EventStore\AggregateEventStream;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;

class MongoAggregateAllEventStream implements AggregateEventStream
{
    /** @var Collection */
    private $collection;

    /** @var int */
    private $version;

    /** @var EventStreamIterator */
    private $eventStreamIterator;

    /** @var AggregateDescriptor */
    private $aggregateDescriptor;

    public function __construct(
        Collection $collection,
        AggregateDescriptor $aggregateDescriptor,
        EventStreamIterator $eventStreamIterator
    )
    {
        $this->collection = $collection;
        $this->version = $this->fetchLatestVersion($aggregateDescriptor);
        $this->eventStreamIterator = $eventStreamIterator;
        $this->aggregateDescriptor = $aggregateDescriptor;
    }

    public function getIterator():\Traversable
    {
        return $this->eventStreamIterator->getIteratorThatExtractsEventsFromDocument(
            $this->getCursorLessThanOrEqualToVersion());
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    private function fetchLatestVersion(AggregateDescriptor $aggregateDescriptor): int
    {
        return (new LastAggregateVersionFetcher())->fetchLatestVersion($this->collection, $aggregateDescriptor);
    }

    private function getCursorLessThanOrEqualToVersion(): Cursor
    {
        return $this->collection->find(
            [
                'streamName' => StreamName::factoryStreamNameFromDescriptor($this->aggregateDescriptor),
                'version'    => [
                    '$lte' => $this->version,
                ],
            ],
            [
                'sort' => [
                    'version' => 1,
                ],
            ]
        );
    }

    public function getCursorGreaterThanToSomeVersion(int $version, int $limit = null): Cursor
    {
        $options = [
            'sort' => [
                'version' => 1,
            ],
        ];

        if ($limit) {
            $options['limit'] = $limit;
        }

        return $this->collection->find(
            [
                'streamName' => StreamName::factoryStreamNameFromDescriptor($this->aggregateDescriptor),
                'version'    => [
                    '$gt' => $version,
                ],
            ],
            $options
        );
    }

    public function count():int
    {
        $pipeline = [];

        $pipeline[] = [
            '$match' => [
                'streamName' => StreamName::factoryStreamNameFromDescriptor($this->aggregateDescriptor),
                'version'    => [
                    '$lte' => $this->version,
                ],
            ],
        ];

        $pipeline[] = [
            '$count' => 'total',
        ];

        return $this->collection->aggregate(
            $pipeline
        )['total'];
    }
}