<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina;


use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Command;

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
                $client = new Client('mongodb://db/');

                $database = $client
                    ->selectDatabase('test');

                iterator_to_array($database->listCollections());

                return $database;
            } catch (\Throwable $exception) {
                sleep(1);

                if ($retries > 200) {
                    echo $exception->getMessage() . "\n";
                    echo "retrying to connect...\n";
                    throw new \InvalidArgumentException("too many retries, " . $exception->getMessage() . "\n");
                }
                continue;
            }
        }
    }

    public function getAdminDatabase(): Database
    {
        $retries = 0;
        while (true) {
            try {
                $retries++;
                $client = new Client('mongodb://db/');

                $database = $client
                    ->selectDatabase('admin');

                iterator_to_array($database->listCollections());

                return $database;
            } catch (\Throwable $exception) {
                echo $exception->getMessage() . "\n";
                echo "retrying to connect...\n";
                sleep(1);

                if ($retries > 200) {
                    throw new \InvalidArgumentException("too many retries, " . $exception->getMessage() . "\n");
                }
                continue;
            }
        }
    }
}