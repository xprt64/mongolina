<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs\EventStore\Mongo\ScheduledCommandStoreTest;


use Gica\Cqrs\EventStore\Mongo\CommandScheduler;
use Gica\Cqrs\EventStore\Mongo\ScheduledCommandStore;
use Gica\Cqrs\Scheduling\ScheduledCommand;
use tests\Gica\Cqrs\MongoTestHelper;

require_once __DIR__ . '/MongoTestHelper.php';

class ScheduledCommandStoreTest extends \PHPUnit_Framework_TestCase
{
    /** @var \MongoDB\Collection */
    private $collection;

    protected function setUp()
    {
        $this->collection = (new MongoTestHelper())->selectCollection('eventStore');
    }

    public function test_appendEventsForAggregate()
    {
        $collection = $this->collection;

        $commandScheduler = new CommandScheduler(
            $collection);

        $scheduledCommandStore = new ScheduledCommandStore(
            $collection);

        $scheduledCommandStore->dropStore();
        $scheduledCommandStore->createStore();

        $command = $this->getMockBuilder(ScheduledCommand::class)
            ->getMock();

        $command->method('getFireDate')
            ->willReturn(new \DateTimeImmutable());

        $command->method('getMessageId')
            ->willReturn('1234');

        /** @var ScheduledCommand $command */
        $commandScheduler->scheduleCommand($command, 'aggregateClass', 123, '');

        $this->assertCount(1, $collection->find()->toArray());

        $processor = $this->getMockBuilder(\stdClass::class)
            ->setMethods(['__invoke'])
            ->getMock();

        $processor->expects($this->once())
            ->method('__invoke')
            ->with($command);

        /** @var callable $processor */
        $scheduledCommandStore->loadAndProcessScheduledCommands($processor);

        $this->assertCount(0, $collection->find()->toArray());
    }

    public function test_appendEventsForAggregateDuplicateCommand()
    {
        $collection = $this->collection;

        $commandScheduler = new CommandScheduler(
            $collection);

        $commandScheduler->dropStore();
        $commandScheduler->createStore();

        $command = $this->getMockBuilder(ScheduledCommand::class)
            ->getMock();

        $command->method('getFireDate')
            ->willReturn(new \DateTimeImmutable());

        $command->method('getMessageId')
            ->willReturn('1234');

        $commandScheduler->scheduleCommand($command, '', '', '');
        $commandScheduler->scheduleCommand($command, '', '', '');
        $commandScheduler->scheduleCommand($command, '', '', '');
        $commandScheduler->scheduleCommand($command, '', '', '');

        $this->assertCount(1, $collection->find()->toArray());
    }
}
