<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Gica\Iterator\IteratorTransformer\IteratorMapper;
use MongoDB\Collection;

class CqrsAllEventByClassesStream implements \Dudulina\EventStore\EventStream
{

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var array
     */
    private $eventClassNames;

    /** @var callable */
    private $eventHydrator;

    public function __construct(
        Collection $collection,
        array $eventClassNames,
        callable $eventHydrator
    )
    {
        $this->collection = $collection;
        $this->eventClassNames = $eventClassNames;
        $this->eventHydrator = $eventHydrator;
    }

    /**
     * @inheritdoc
     */
    public function getIterator():\Traversable
    {
        return $this->getIteratorForEvents($this->getCursorForEvents());
    }

    public function getCursorForEvents(): \Traversable
    {
        $pipeline = $this->getEventsPipeline();

        $options = [
            'noCursorTimeout' => true,
        ];

        return $this->collection->aggregate(
            $pipeline,
            $options
        );
    }

    private function getFilter(): array
    {
        $filter = [];

        if ($this->eventClassNames) {
            $filter['events.@classes.event'] = [
                '$in' => $this->eventClassNames,
            ];
        }

        return $filter;
    }

    private function getIteratorForEvents($documents): \Traversable
    {
        $events = (new IteratorMapper(function ($document) {
            return ($this->eventHydrator)($document);
        }))($documents);

        return $events;
    }

    private function getEventsPipeline(): array
    {
        $pipeline = [];

        if ($this->getFilter()) {
            $pipeline[] = [
                '$match' => $this->getFilter(),
            ];
        }

        $pipeline[] = [
            '$unwind' => [
                'path'              => '$events',
                'includeArrayIndex' => 'index',
            ],
        ];

        if ($this->getFilter()) {
            $pipeline[] = [
                '$match' => $this->getFilter(),
            ];
        }
        return $pipeline;
    }

    public function count():int
    {
        $pipeline = $this->getEventsPipeline();

        $pipeline[] = [
            '$count' => 'total',
        ];

        $options = [
            'noCursorTimeout' => true,
        ];

        return iterator_to_array($this->collection->aggregate(
            $pipeline,
            $options
        ))[0]['total'];
    }

}