<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Gica\Cqrs\EventStore\Mongo;

use Gica\Cqrs\Event\EventWithMetaData;
use Gica\Iterator\IteratorTransformer\IteratorExpander;

/**
 * @property EventSerializer $eventSerializer
 */
trait EventStreamIteratorTrait
{
    use DocumentParserTrait;

    private function getIteratorThatExtractsEventsFromDocument($cursor): \Traversable
    {
        $expanderCallback = function ($document) {
            $metaData = $this->extractMetaDataFromDocument($document);

            foreach ($document['events'] as $index => $eventSubDocument) {
                try {
                    $event = $this->eventSerializer->deserializeEvent($eventSubDocument['eventClass'], $eventSubDocument['payload']);

                    if ($eventSubDocument['id']) {
                        $metaData = $metaData->withEventId($eventSubDocument['id']);
                    }

                    if ($document['sequence']) {
                        $metaData = $metaData->withSequenceAndIndex($document['sequence'], $index);
                    }

                    yield new EventWithMetaData($event, $metaData->withEventId($eventSubDocument['id']));
                } catch (\Throwable $exception) {

                }
            }

        };

        $generator = new IteratorExpander($expanderCallback);

        return $generator($cursor);
    }
}