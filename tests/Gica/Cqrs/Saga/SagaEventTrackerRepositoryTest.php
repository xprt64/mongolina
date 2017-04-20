<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs\Saga\SagaEventTrackerRepositoryTest;

use Gica\Cqrs\EventStore\Mongo\Saga\SagaEventTrackerRepository;
use Gica\Cqrs\Saga\EventOrder;
use Gica\Cqrs\Saga\SagaEventTrackerRepository\ConcurentEventProcessingException;
use tests\Gica\Cqrs\MongoTestHelper;

require_once __DIR__ . '/../MongoTestHelper.php';

class SagaEventTrackerRepositoryTest extends \PHPUnit_Framework_TestCase
{
    /** @var SagaEventTrackerRepository */
    private $sut;

    protected function setUp()
    {
        $collectionStart = (new MongoTestHelper())->selectCollection('start');

        $this->sut = new SagaEventTrackerRepository(
            $collectionStart
        );

        $this->sut->createStorage();
    }

    public function test_startProcessingEventBySaga()
    {
        $eventId = "1";

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            $eventId
        ));

        $this->sut->startProcessingEventBySaga('someId', $eventId);

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            $eventId
        ));
    }

    public function test_endProcessingEventBySaga()
    {
        $eventId = "1";

        $this->sut->startProcessingEventBySaga('someId', $eventId);

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));

        $this->sut->endProcessingEventBySaga('someId', $eventId);

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));
    }

    public function test_startProcessingEventBySaga_ConcurentEventProcessingException()
    {
        $this->expectException(ConcurentEventProcessingException::class);

        $this->sut->startProcessingEventBySaga('someId', "1");
        $this->sut->startProcessingEventBySaga('someId', "1");
    }

    public function test_clearProcessingEventBySaga()
    {
        $eventId = "1";

        $this->sut->startProcessingEventBySaga('someId', $eventId);
        $this->sut->clearProcessingEventBySaga('someId', $eventId);

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));
    }
}
