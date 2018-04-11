<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\EventStore\TailableEventStream;
use MongoDB\Client;
use MongoDB\Operation\Find;
use Mongolina\EventsCommit\CommitSerializer;

class TailableMongoEventStream implements TailableEventStream
{
    /**
     * @var CommitSerializer
     */
    private $commitSerializer;
    /**
     * @var Client
     */
    private $mongoClient;
    /**
     * @var string
     */
    private $eventStoreNamespace;

    public function __construct(
        Client $mongoClient,
        CommitSerializer $commitSerializer,
        string $eventStoreNamespace = 'eventStore.eventStore'
    )
    {
        $this->commitSerializer = $commitSerializer;
        $this->mongoClient = $mongoClient;
        $this->eventStoreNamespace = $eventStoreNamespace;
    }

    /**
     * @inheritdoc
     */
    public function tail(callable $callback, $eventClasses = [], string $afterSequence = null): void
    {
        $collection = $this->mongoClient->local->selectCollection('oplog.rs');

        $lastSequence = $afterSequence ? EventSequence::fromString($afterSequence) : null;

        while (true) {
            $query = [];
            if ($eventClasses) {
                $query['o.' . MongoEventStore::EVENTS_EVENT_CLASS] = ['$in' => $eventClasses];
            }
            if ($lastSequence) {
                $query['ts'] = ['$gt' => $lastSequence->getTimestamp()];
            }
            $query['op'] = 'i';//operation = insert
            $query['ns'] = $this->eventStoreNamespace;//namespace = eventStoreDatabase.eventStoreCollection
            $cursor = $collection->find($query, [
                'cursorType'     => Find::TAILABLE_AWAIT,
                'maxAwaitTimeMS' => 100,
                'oplogReplay'    => true,
            ]);
            $iterator = new \IteratorIterator($cursor);
            $iterator->rewind();
            while (true) {
                if ($iterator->valid()) {
                    $doc = $iterator->current();
                    $lastSequence = $this->processCommit($callback, $doc['o'], $lastSequence);
                }
                $iterator->next();
            }
            sleep(1);
        }
    }

    private function processCommit(callable $callback, $document, EventSequence $afterSequence = null)
    {
        foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubdocument) {
            $eventWithMetaData = $this->commitSerializer->extractEventFromSubDocument($eventSubdocument, $index, $document);
            $eventSequence = EventSequence::fromString($eventWithMetaData->getMetaData()->getSequence());

            if ($afterSequence && $eventSequence->isAfter($afterSequence)) {
                $callback($eventWithMetaData);
            }
            $afterSequence = $eventSequence;
        }
        return $afterSequence;
    }
}