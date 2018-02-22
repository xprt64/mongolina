<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;

class StreamName
{
    public static function factoryStreamName(string $aggregateClass, $aggregateId)
    {
        return self::factoryStreamNameFromDescriptor(new AggregateDescriptor($aggregateId, $aggregateClass));
    }

    public static function factoryStreamNameFromDescriptor(AggregateDescriptor $aggregateDescriptor)
    {
        return substr(hash('sha256', $aggregateDescriptor->getAggregateClass() . $aggregateDescriptor->getAggregateId()), 0, 24);
    }
}