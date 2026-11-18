<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\EventStore\AggregateEventStream;
use MongoDB\Collection;

class MongoAggregateAllEventStreamFactory
{
    public function __construct(private EventStreamIterator $eventStreamIterator)
    {
    }

    public function createStream(Collection $collection, AggregateDescriptor $aggregateDescriptor): AggregateEventStream
    {
        return new MongoAggregateAllEventStream($collection, $aggregateDescriptor, $this->eventStreamIterator);
    }
}