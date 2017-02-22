<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Event\EventWithMetaData;
use Gica\Cqrs\EventStore\ByClassNamesEventStream;
use Gica\Iterator\IteratorTransformer\IteratorExpander;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;

class MongoAllEventByClassesStream implements ByClassNamesEventStream
{
    use EventStreamIteratorTrait;

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var EventSerializer
     */
    private $eventSerializer;
    /**
     * @var array
     */
    private $eventClassNames;

    /** @var int|null */
    private $limit = null;

    /** @var int|null */
    private $skip;

    public function __construct(
        Collection $collection,
        array $eventClassNames,
        EventSerializer $eventSerializer
    )
    {
        $this->collection = $collection;
        $this->eventSerializer = $eventSerializer;
        $this->eventClassNames = $eventClassNames;
    }

    /**
     * @inheritdoc
     */
    public function limitCommits(int $limit)
    {
        $this->limit = $limit;
    }

    /**
     * @inheritdoc
     */
    public function skipCommits(int $numberOfCommitsToBeSkipped)
    {
        $this->skip = $numberOfCommitsToBeSkipped;
    }

    /**
     * @inheritdoc
     */
    public function getIterator()
    {
        $cursor = $this->getCursor();

        return $this->getIteratorThatExtractsInterestingEventsFromDocument($cursor);
    }

    private function getCursor(): Cursor
    {
        $options = [
            'sort' => [
                'sequence' => 1,
            ],
        ];

        if ($this->limit > 0) {
            $options['limit'] = $this->limit;
        }

        if ($this->skip > 0) {
            $options['skip'] = $this->skip;
        }

        $cursor = $this->collection->find(
            [
                MongoEventStore::EVENTS_EVENT_CLASS => [
                    '$in' => $this->eventClassNames,
                ],
            ],
            $options
        );

        return $cursor;
    }

    private function getIteratorThatExtractsInterestingEventsFromDocument($cursor): \Traversable
    {
        $expanderCallback = function ($document) {
            $metaData = $this->extractMetaDataFromDocument($document);

            foreach ($document['events'] as $eventSubDocument) {
                if (!$this->isInterestingEvent($eventSubDocument[MongoEventStore::EVENT_CLASS])) {
                    continue;
                }

                $event = $this->eventSerializer->deserializeEvent($eventSubDocument[MongoEventStore::EVENT_CLASS], $eventSubDocument['payload']);

                yield new EventWithMetaData($event, $metaData);
            }
        };

        $generator = new IteratorExpander($expanderCallback);

        return $generator($cursor);
    }

    private function isInterestingEvent($eventClass)
    {
        return in_array($eventClass, $this->eventClassNames);
    }
}