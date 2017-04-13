<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs\Saga\State;

use Gica\Cqrs\EventStore\Mongo\Saga\State\StateManager;
use MongoDB\Client;

class StateManagerTest extends \PHPUnit_Framework_TestCase
{
    /** @var \MongoDB\Collection */
    private $collection;

    protected function setUp()
    {
        $retries = 0;
        while (true) {
            try {
                $retries++;
                $this->collection = (new Client('mongodb://db'))
                    ->selectCollection('test', 'test');

                $this->collection->findOne([]);
                break;
            } catch (\Throwable $exception) {
                //echo $exception->getMessage() . "\n";
                sleep(1);

                if ($retries > 20) {
                    die("too many retries, " . $exception->getMessage() . "\n");
                }
                continue;
            }
        }
    }

    public function test_loadState()
    {
        $sut = new StateManager($this->collection);

        $state = $sut->loadState(MyClass::class, 123);

        $this->assertNull($state);
    }

    public function test_updateState()
    {
        $sut = new StateManager($this->collection);

        //first state update
        $sut->updateState(123, function (?MyClass $state) {
            if (!$state) {
                $state = new MyClass();
            }
            $state->someValue = 345;
            return $state;
        });

        $state = $sut->loadState(MyClass::class, 123);

        $this->assertSame(345, $state->someValue);

        //second state update

        $sut->updateState(123, function (?MyClass $state) {
            $state->someValue = 567;
            return $state;
        });

        $state = $sut->loadState(MyClass::class, 123);

        $this->assertSame(567, $state->someValue);

        $this->assertSame(1, $sut->debugGetVersionCountForState(MyClass::class, 123));
    }

}

class MyClass
{

}