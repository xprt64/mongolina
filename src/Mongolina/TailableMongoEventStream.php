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
        while (true) {
            $query = [];
            if ($eventClasses) {
                $query['o.' . MongoEventStore::EVENTS_EVENT_CLASS] = ['$in' => $eventClasses];
            }
            if ($afterSequence) {
                $sequence = EventSequence::fromString($afterSequence);
                $query['ts'] = ['$gt' => $sequence->getTimestamp()];
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
                    $afterSequence = $doc['ts'];
                    $this->processCommit($callback, $doc['o']);
                }
                $iterator->next();
            }
            sleep(1);
        }
    }

    private function processCommit(callable $callback, $document)
    {
        foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubdocument) {
            $callback(
                $this->commitSerializer->extractEventFromSubDocument($eventSubdocument, $index, $document)
            );
        }
    }
}