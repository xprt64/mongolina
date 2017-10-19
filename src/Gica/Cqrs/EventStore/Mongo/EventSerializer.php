<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Gica\Cqrs\EventStore\Mongo;


class EventSerializer
{
    public function serializeEvent($event)
    {
        return serialize($event);
    }

    public function deserializeEvent($eventClass, $eventPayload)
    {
        return unserialize($eventPayload);
    }
}