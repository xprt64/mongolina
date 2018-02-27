<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Mongolina\EventsCommit\CommitSerializer;
use MongoDB\Collection;

class MongoAllEventByClassesStreamFactory
{
    /**
     * @var CommitSerializer
     */
    private $commitSerializer;

    public function __construct(
        CommitSerializer $commitSerializer
    )
    {
        $this->commitSerializer = $commitSerializer;
    }

    public function createStream(Collection $collection, array $eventClassNames): MongoAllEventByClassesStream
    {
        return new MongoAllEventByClassesStream(
            $collection, $eventClassNames, $this->commitSerializer
        );
    }
}