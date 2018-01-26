<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs;


use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;

class MongoTestHelper
{
    public function selectCollection(string $collectionName): Collection
    {
        $database = $this->getDatabase();

        $collection = $database->selectCollection($collectionName);

        $collection->findOne([]);
        $collection->deleteMany([]);

        return $collection;
    }

    public function getDatabase(): Database
    {
        $retries = 0;
        while (true) {
            try {
                $retries++;
                $database = (new Client('mongodb://db'))
                    ->selectDatabase('test');

                return $database;
            } catch (\Throwable $exception) {
                echo $exception->getMessage() . "\n";
                echo "retrying to connect...\n";
                sleep(1);

                if ($retries > 20) {
                    die("too many retries, " . $exception->getMessage() . "\n");
                }
                continue;
            }
        }
    }
}