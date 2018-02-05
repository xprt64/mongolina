<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\Saga\SagaEventTrackerRepositoryTest;

use Dudulina\EventProcessing\ConcurentEventProcessingException;
use Mongolina\Saga\SagaEventTrackerRepository;
use tests\Dudulina\MongoTestHelper;

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

    public function test_startProcessingEvent()
    {
        $eventId = "1";

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            $eventId
        ));

        $this->sut->startProcessingEvent('someId', $eventId);

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            $eventId
        ));
    }

    public function test_endProcessingEvent()
    {
        $eventId = "1";

        $this->sut->startProcessingEvent('someId', $eventId);

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));

        $this->sut->endProcessingEvent('someId', $eventId);

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));
    }

    public function test_startProcessingEvent_ConcurentEventProcessingException()
    {
        $this->expectException(ConcurentEventProcessingException::class);

        $this->sut->startProcessingEvent('someId', "1");
        $this->sut->startProcessingEvent('someId', "1");
    }

    public function test_clearProcessingEvent()
    {
        $eventId = "1";

        $this->sut->startProcessingEvent('someId', $eventId);
        $this->sut->clearProcessingEvent('someId', $eventId);

        $this->assertSame(false, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            $eventId
        ));
    }

    public function test_getAllInProgressProcessingEvents()
    {
        $this->assertCount(0, $this->sut->getAllInProgressProcessingEvents('someId'));

        $this->sut->startProcessingEvent('someId', "1");
        $this->assertCount(1, $this->sut->getAllInProgressProcessingEvents('someId'));

        $this->sut->startProcessingEvent('someId', "2");
        $this->assertCount(2, $this->sut->getAllInProgressProcessingEvents('someId'));

        $this->sut->endProcessingEvent('someId', "2");
        $this->assertCount(1, $this->sut->getAllInProgressProcessingEvents('someId'));

        /** @var \Dudulina\EventProcessing\InProgressProcessingEvent[]  $events */
        $events = iterator_to_array($this->sut->getAllInProgressProcessingEvents('someId'), false);

        $event = reset($events);

        $this->assertSame("1", $event->getEventId());
    }
}
