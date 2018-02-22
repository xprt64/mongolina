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

class MongoAggregateAllEventStreamFactory
{
    /**
     * @var EventStreamIterator
     */
    private $eventStreamIterator;

    public function __construct(
        EventStreamIterator $eventStreamIterator
    )
    {
        $this->eventStreamIterator = $eventStreamIterator;
    }

    public function createStream(Collection $collection, AggregateDescriptor $aggregateDescriptor): AggregateEventStream
    {
        return new MongoAggregateAllEventStream($collection, $aggregateDescriptor, $this->eventStreamIterator);
    }
}