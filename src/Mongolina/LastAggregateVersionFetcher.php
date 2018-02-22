<?php
/******************************************************************************
 * Copyright (c) 2016 Constantin Galbenu <gica.galbenu@gmail.com>             *
 ******************************************************************************/

namespace Mongolina;


use Dudulina\Aggregate\AggregateDescriptor;
use MongoDB\BSON\ObjectID;
use MongoDB\Collection;

class LastAggregateVersionFetcher
{
    public function fetchLatestVersion(Collection $collection, AggregateDescriptor $aggregateDescriptor): int
    {
        $cursor = $collection->find(
            [
                'streamName' => new ObjectID(StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor)),
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