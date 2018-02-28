<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\EventStore\Mongo\MongoEventStoreTest;

require_once __DIR__ . '/MongoTestHelper.php';

use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\Event\MetaData;
use Gica\Serialize\ObjectSerializer\CompositeSerializer;
use Gica\Serialize\ObjectSerializer\ObjectSerializer;
use Mongolina\EventsCommit\CommitSerializer;
use Gica\Lib\ObjectToArrayConverter;
use Gica\Types\Guid;
use Mongolina\EventSerializer;
use Mongolina\EventStreamIterator;
use Mongolina\MongoAggregateAllEventStreamFactory;
use Mongolina\MongoAllEventByClassesStreamFactory;
use Mongolina\MongoEventStore;
use tests\Dudulina\MongoTestHelper;

class MongoEventStoreTest extends \PHPUnit_Framework_TestCase
{
    const AGGREGATE_CLASS = 'aggClass';
    /** @var \MongoDB\Collection */
    private $collection;

    protected function setUp()
    {
        $this->collection = (new MongoTestHelper())->selectCollection('eventStore');
    }

    public function test_appendEventsForAggregate()
    {
        $eventStore = $this->factoryEventStore();

        $eventStore->dropStore();
        $eventStore->createStore();

        $aggregateId = 123;
        $aggregateClass = self::AGGREGATE_CLASS;

        $expectedEventStream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor());

        $events = $this->wrapEventsWithMetadata($aggregateClass, $aggregateId, [new Event1(11), new Event2(22)]);

        $eventStore->appendEventsForAggregate($this->factoryAggregateDescriptor(), $events, $expectedEventStream);

        $this->assertCount(1, $this->collection->find()->toArray());

        $stream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor());

        $events = iterator_to_array($stream->getIterator());

        $this->assertCount(2, $events);

        $this->assertInstanceOf(Event1::class, $events[0]->getEvent());
        $this->assertInstanceOf(Event2::class, $events[1]->getEvent());
    }

    private function factoryAggregateDescriptor(): AggregateDescriptor
    {
        return new AggregateDescriptor(123, self::AGGREGATE_CLASS);
    }

    private function wrapEventsWithMetadata($aggregateClass, $aggregateId, $events)
    {
        return array_map(function ($event) use ($aggregateClass, $aggregateId) {
            return $this->wrapEventWithMetadata($aggregateClass, $aggregateId, $event);
        }, $events);
    }

    private function wrapEventWithMetadata($aggregateClass, $aggregateId, $event)
    {
        return new EventWithMetaData(
            $event,
            (new MetaData(
                $aggregateId,
                $aggregateClass,
                new \DateTimeImmutable(),
                null
            ))->withEventId(Guid::generate())
        );
    }

    /**
     * @expectedException \Dudulina\EventStore\Exception\ConcurrentModificationException
     */
    public function test_appendEventsForAggregateShouldNotWriteTwiceTheSameEvents()
    {
        $eventStore = $this->factoryEventStore();

        $eventStore->dropStore();
        $eventStore->createStore();

        $aggregateId = 123;

        $events = $this->wrapEventsWithMetadata($aggregateId, self::AGGREGATE_CLASS, [new Event1(11), new Event2(22)]);

        $expectedEventStream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor());

        $eventStore->appendEventsForAggregate($this->factoryAggregateDescriptor(), $events, $expectedEventStream);

        $eventStore->appendEventsForAggregate($this->factoryAggregateDescriptor(), $events, $expectedEventStream);
    }

    private function factoryEventStore(): MongoEventStore
    {
        return new MongoEventStore(
            $this->collection,
            new MongoAggregateAllEventStreamFactory(
                new EventStreamIterator(
                    $this->factoryCommitSerializer()
                )
            ),
            new MongoAllEventByClassesStreamFactory(
                $this->factoryCommitSerializer()
            ),
            $this->factoryCommitSerializer()
        );
    }

    private function factoryCommitSerializer(): CommitSerializer
    {
        return new CommitSerializer(
            new EventSerializer(),
            new ObjectSerializer(
                new CompositeSerializer([])
            )
        );
    }
}

class Event1 implements \Dudulina\Event
{
    private $field1;

    public function __construct($field1)
    {
        $this->field1 = $field1;
    }

    /**
     * @return mixed
     */
    public function getField1()
    {
        return $this->field1;
    }


}

class Event2 implements \Dudulina\Event
{
    private $field2;

    public function __construct($field2)
    {
        $this->field2 = $field2;
    }

    /**
     * @return mixed
     */
    public function getField2()
    {
        return $this->field2;
    }

}
