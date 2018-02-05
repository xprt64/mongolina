<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use MongoDB\BSON\ObjectID;
use MongoDB\Collection;

class LastAggregateVersionFetcher
{
    public function fetchLatestVersion(Collection $collection, string $aggregateClass, $aggregateId):int
    {
        $cursor = $collection->find(
            [
//                'aggregateId' => (string)$aggregateId,
//                'aggregateClass' => $aggregateClass,
                'streamName' => new ObjectID(StreamName::factoryStreamName($aggregateClass, $aggregateId)),
            ],
            [
                'sort'  => [
                    'version' => -1,
                ],
                'limit' => 1,
            ]
        );

        $documents = $cursor->toArray();
        if ($documents) {
            $last = array_pop($documents);
            $version = (int)$last['version'];
        } else {
            $version = 0;
        }

        return $version;
    }
}