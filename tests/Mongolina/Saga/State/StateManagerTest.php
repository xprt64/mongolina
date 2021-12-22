<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\Saga\State;

use Mongolina\Saga\State\StateManager;
use tests\Dudulina\MongoTestHelper;

require_once __DIR__ . '/../../MongoTestHelper.php';

class StateManagerTest extends \PHPUnit\Framework\TestCase
{
    private const STATE_ID = 123;

    /** @var \MongoDB\Database */
    private $database;

    /** @var \MongoDB\Database */
    private $adminDatabase;

    protected function setUp():void
    {
        $this->database = (new MongoTestHelper())->getDatabase();
        $this->adminDatabase = (new MongoTestHelper())->getAdminDatabase();

        foreach ($this->database->listCollections() as $collection) {
            $this->database->dropCollection($collection->getName());
        }
    }

    public function test_loadState()
    {
        $sut = new StateManager($this->database, $this->adminDatabase);

        $state = $sut->loadState(MyClass::class, 123);

        $this->assertNull($state);
    }

    public function test_updateState()
    {
        $sut = new StateManager($this->database, $this->adminDatabase);

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

    public function test_moveStateToNamespace()
    {
        $sut = new StateManager($this->database, $this->adminDatabase);
        $sourceNamespace = 'src';
        $destinationNamespace = 'dst';
        $storageName = 'test_namespace';
        $sut->createStorage($storageName, $sourceNamespace);
        $sut->createStorage($storageName, $destinationNamespace);

        $updater = function (\stdClass $state = null) {
            return 345;
        };
        $sut->updateState(self::STATE_ID, $updater, $storageName, $sourceNamespace);
        $this->assertEquals(345, $sut->loadState(\stdClass::class, self::STATE_ID, $storageName, $sourceNamespace));
        $this->assertNull($sut->loadState(\stdClass::class, self::STATE_ID, $storageName, $destinationNamespace));
        $sut->moveEntireNamespace($sourceNamespace, $destinationNamespace);
        $this->assertNull($sut->loadState(\stdClass::class, self::STATE_ID, $storageName, $sourceNamespace));
        $this->assertEquals(345, $sut->loadState(\stdClass::class, self::STATE_ID, $storageName, $destinationNamespace));
    }
}

class MyClass
{

}