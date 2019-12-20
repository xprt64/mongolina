<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\EventStore\Mongo\ScheduledCommandStoreTest;


use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Command\CommandMetadata;
use Mongolina\CommandScheduler;
use Mongolina\ScheduledCommandStore;
use Dudulina\Scheduling\ScheduledCommand;
use Gica\Types\Guid;
use tests\Dudulina\MongoTestHelper;

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
        $commandMetadata = ['a' => 'b', 'c' => 1];

        $commandScheduler->scheduleCommand($command, $this->factoryAggregateDescriptor(), $commandMetadata);

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

        $commandMetadata = (new CommandMetadata())->withCorrelationId(Guid::generate());

        $commandScheduler->scheduleCommand($command, $this->factoryAggregateDescriptor(), $commandMetadata);
        $commandScheduler->scheduleCommand($command, $this->factoryAggregateDescriptor(), $commandMetadata);
        $commandScheduler->scheduleCommand($command, $this->factoryAggregateDescriptor(), $commandMetadata);
        $commandScheduler->scheduleCommand($command, $this->factoryAggregateDescriptor(), $commandMetadata);

        $this->assertCount(1, $collection->find()->toArray());
    }

    private function factoryAggregateDescriptor(): AggregateDescriptor
    {
        return new AggregateDescriptor(123, 'aggregateClass');
    }
}
