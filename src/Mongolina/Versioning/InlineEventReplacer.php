<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\Versioning;


use MongoDB\Collection;
use Mongolina\EventsCommit\CommitSerializer;
use Mongolina\MongoEventStore;
use Mongolina\Versioning\InlineEventMigrator\EventReplacer;

class InlineEventReplacer
{

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var CommitSerializer
     */
    private $commitSerializer;

    public function __construct(
        Collection $collection,
        CommitSerializer $commitSerializer
    )
    {
        $this->collection = $collection;
        $this->commitSerializer = $commitSerializer;
    }

    public function replaceEvent(EventReplacer $replacer, string $eventClass, callable $progressCallback = null)
    {
        $cursor = $this->collection->find([
            MongoEventStore::EVENTS_EVENT_CLASS => $eventClass,
        ]);

        foreach ($cursor as $commit) {
            $this->updateCommit($commit, $replacer, $eventClass);

            if ($progressCallback) {
                $progressCallback();
            }
        }
    }

    private function updateCommit($commit, EventReplacer $replacer, string $eventClass)
    {
        $commitUpdated = false;

        foreach ($commit[MongoEventStore::EVENTS] as &$eventRow) {
            if ($eventClass !== $eventRow[MongoEventStore::EVENT_CLASS]) {
                continue;
            }
            $oldEvent = $this->commitSerializer->extractEventFromSubDocument($eventRow, $commit);
            $newEvent = $replacer->replaceEvent($oldEvent);
            if ($oldEvent === $newEvent) {
                continue;
            }
            $eventRow = $this->commitSerializer->packEvent($newEvent);
            $commitUpdated = true;
        }
        unset($eventRow);

        if ($commitUpdated) {
            $this->collection->updateOne([
                '_id' => $commit['_id'],
            ], [
                '$set' => $commit,
            ]);
        }
    }
}