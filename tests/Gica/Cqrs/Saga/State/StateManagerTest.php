<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\Saga\State;

use Mongolina\Saga\State\StateManager;
use tests\Dudulina\MongoTestHelper;

require_once __DIR__ . '/../../MongoTestHelper.php';

class StateManagerTest extends \PHPUnit_Framework_TestCase
{
    /** @var \MongoDB\Database */
    private $database;

    protected function setUp()
    {
        $this->database = (new MongoTestHelper())->getDatabase();

        foreach ($this->database->listCollections() as $collection) {
            $this->database->dropCollection($collection->getName());
        }
    }

    public function test_loadState()
    {
        $sut = new StateManager($this->database);

        $state = $sut->loadState(MyClass::class, 123);

        $this->assertNull($state);
    }

    public function test_updateState()
    {
        $sut = new StateManager($this->database);

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