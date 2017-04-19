<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs;


use MongoDB\Client;
use MongoDB\Collection;

class MongoTestHelper
{
    public function selectCollection(string $collectionName): Collection
    {
        $retries = 0;
        while (true) {
            try {
                //echo "trying to connect...\n";
                $retries++;
                $collection = (new Client('mongodb://db'))
                    ->selectCollection('test', $collectionName);

                $collection->findOne([]);
                $collection->deleteMany([]);

                return $collection;
            } catch (\Throwable $exception) {
                echo $exception->getMessage() . "\n";
                sleep(1);

                if ($retries > 20) {
                    die("too many retries, " . $exception->getMessage() . "\n");
                }
                continue;
            }
        }
    }
}