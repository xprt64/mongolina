<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\EventStore\TailableEventStream;
use Mongolina\EventsCommit\CommitSerializer;

class TailableMongoEventStream implements TailableEventStream
{
    /**
     * @var CommitSerializer
     */
    private $commitSerializer;
    /**
     * @var \MongoDB\Client
     */
    private $mongoClient;
    /**
     * @var string
     */
    private $eventStoreNamespace;

    public function __construct(
        \MongoDB\Client $mongoClient,
        CommitSerializer $commitSerializer,
        string $eventStoreNamespace = 'eventStore.eventStore'
    )
    {
        $this->commitSerializer = $commitSerializer;
        $this->mongoClient = $mongoClient;
        $this->eventStoreNamespace = $eventStoreNamespace;
    }

    /**
     * @param callable $callback function(EventWithMetadata)
     * @param string[] $eventClasses
     * @param mixed|null $afterTimestamp
     */
    public function tail(callable $callback, $eventClasses = [], $afterTimestamp = null): void
    {
        $collection = $this->mongoClient->local->selectCollection('oplog.rs');;

        while (true) {
            $query = [];

            if ($eventClasses) {
                $query['o.events.eventClass'] = ['$in' => $eventClasses];
            }

            if ($afterTimestamp) {
                $query['ts'] = ['$gt' => $afterTimestamp];
            }

            $query['op'] = 'i';
            $query['ns'] = $this->eventStoreNamespace;

            $cursor = $collection->find($query, [
                'cursorType'     => \MongoDB\Operation\Find::TAILABLE_AWAIT,
                'maxAwaitTimeMS' => 100,
                'oplogReplay'    => true,
            ]);

            $iterator = new \IteratorIterator($cursor);

            $iterator->rewind();

            while (true) {
                if ($iterator->valid()) {
                    $doc = $iterator->current();
                    $afterTimestamp = $doc['ts'];
                    $this->processCommit($callback, $doc["o"]);
                }

                $iterator->next();
            }

            sleep(1);
        }
    }

    private function processCommit(callable $callback, $document)
    {
        foreach ($document['events'] as $eventSubdocument) {
            $callback(
                $this->commitSerializer->extractEventFromSubDocument($eventSubdocument, $document)
            );
        }
    }
}