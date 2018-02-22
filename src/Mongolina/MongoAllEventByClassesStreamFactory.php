<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use MongoDB\Collection;

class MongoAllEventByClassesStreamFactory
{
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var DocumentParser
     */
    private $documentParser;

    public function __construct(
        EventSerializer $eventSerializer,
        DocumentParser $documentParser
    )
    {
        $this->eventSerializer = $eventSerializer;
        $this->documentParser = $documentParser;
    }

    public function createStream(Collection $collection, array $eventClassNames): MongoAllEventByClassesStream
    {
        return new MongoAllEventByClassesStream(
            $collection, $eventClassNames, $this->eventSerializer, $this->documentParser
        );
    }
}