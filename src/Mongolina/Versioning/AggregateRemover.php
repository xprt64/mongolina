<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\Versioning;


use Dudulina\Aggregate\AggregateDescriptor;
use MongoDB\Collection;
use Mongolina\MongoEventStore;
use Mongolina\StreamName;

class AggregateRemover
{

    public function __construct(private Collection $collection)
    {
    }

    public function removeAggregate(AggregateDescriptor $aggregateDescriptor)
    {
        $this->collection->deleteMany([
            MongoEventStore::STREAM_NAME => StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor),
        ]);
    }
}