<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Gica\Cqrs\EventStore\Mongo;


class StreamName
{
    public static function factoryStreamName(string $aggregateClass, $aggregateId)
    {
        return substr(hash('sha256', $aggregateClass . $aggregateId), 0, 24);
    }
}