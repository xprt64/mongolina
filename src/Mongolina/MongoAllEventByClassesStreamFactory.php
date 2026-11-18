<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Mongolina\EventsCommit\CommitSerializer;
use MongoDB\Collection;

class MongoAllEventByClassesStreamFactory
{
    public function __construct(private CommitSerializer $commitSerializer)
    {
    }

    public function createStream(Collection $collection, array $eventClassNames): MongoAllEventByClassesStream
    {
        return new MongoAllEventByClassesStream(
            $collection, $eventClassNames, $this->commitSerializer
        );
    }
}