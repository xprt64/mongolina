<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use MongoDB\BSON\ObjectId;

class StreamName
{
    public static function factoryStreamNameFromDescriptor(AggregateDescriptor $aggregateDescriptor): ObjectId
    {
        return new ObjectId(substr(hash('sha256', $aggregateDescriptor->getAggregateClass() . $aggregateDescriptor->getAggregateId()), 0, 24));
    }
}