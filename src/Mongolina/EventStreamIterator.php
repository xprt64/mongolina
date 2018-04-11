<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;

use Mongolina\EventsCommit\CommitSerializer;
use Gica\Iterator\IteratorTransformer\IteratorExpander;

class EventStreamIterator
{
    /**
     * @var \Mongolina\EventsCommit\CommitSerializer
     */
    private $commitSerializer;

    public function __construct(
        CommitSerializer $commitSerializer
    )
    {
        $this->commitSerializer = $commitSerializer;
    }

    public function getIteratorThatExtractsEventsFromDocument($cursor): \Traversable
    {
        $expanderCallback = function ($document) {
            foreach ($document[MongoEventStore::EVENTS] as $index => $eventSubDocument) {
                try {
                    yield $this->commitSerializer->extractEventFromSubDocument($eventSubDocument, $index, $document);
                } catch (\Throwable $exception) {
                    var_dump($exception->getMessage());
                    var_dump($exception->getFile());
                    var_dump($exception->getLine());
                    die();
                }
            }
        };

        $generator = new IteratorExpander($expanderCallback);

        return $generator($cursor);
    }
}