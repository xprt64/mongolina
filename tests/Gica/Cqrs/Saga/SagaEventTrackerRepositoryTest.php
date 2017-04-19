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
    /** @var \MongoDB\Collection */
    private $collectionStart;

    /** @var \MongoDB\Collection */
    private $collectionEnd;

    /** @var SagaEventTrackerRepository */
    private $sut;

    protected function setUp()
    {
        $this->collectionStart = (new MongoTestHelper())->selectCollection('start');
        $this->collectionEnd = (new MongoTestHelper())->selectCollection('end');

        $this->sut = new SagaEventTrackerRepository(
            $this->collectionStart,
            $this->collectionEnd
        );

        $this->sut->createStorage();
    }

    public function test_startProcessingEventBySaga()
    {
        $this->assertSame(false, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            new EventOrder(1, 2)
        ));

        $this->assertNull($this->sut->getLastStartedEventSequenceAndIndex(
            'someId'
        ));

        $this->sut->startProcessingEventBySaga('someId', new EventOrder(1, 2));

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyStarted(
            'someId',
            new EventOrder(1, 2)
        ));

        $last = $this->sut->getLastStartedEventSequenceAndIndex('someId');

        $this->assertInstanceOf(EventOrder::class, $last);

        $this->assertSame(1, $last->getSequence());
        $this->assertSame(2, $last->getIndex());


        // process another
        $this->sut->startProcessingEventBySaga('someId', new EventOrder(2, 3));

        $last = $this->sut->getLastStartedEventSequenceAndIndex('someId');

        $this->assertInstanceOf(EventOrder::class, $last);

        $this->assertSame(2, $last->getSequence());
        $this->assertSame(3, $last->getIndex());
    }

    public function test_endProcessingEventBySaga()
    {
        $this->assertSame(false, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            new EventOrder(1, 2)
        ));

        $this->assertNull($this->sut->getLastStartedEventSequenceAndIndex(
            'someId'
        ));

        $this->sut->endProcessingEventBySaga('someId', new EventOrder(1, 2));

        $this->assertSame(true, $this->sut->isEventProcessingAlreadyEnded(
            'someId',
            new EventOrder(1, 2)
        ));
    }

    public function test_startProcessingEventBySaga_ConcurentEventProcessingException()
    {
        $this->expectException(ConcurentEventProcessingException::class);

        $this->sut->startProcessingEventBySaga('someId', new EventOrder(1, 2));
        $this->sut->startProcessingEventBySaga('someId', new EventOrder(1, 2));
    }
}
