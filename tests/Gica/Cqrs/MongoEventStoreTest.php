<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Gica\Cqrs\EventStore\Mongo\MongoEventStoreTest;

require_once __DIR__ . '/MongoTestHelper.php';
use Gica\Cqrs\Event\EventWithMetaData;
use Gica\Cqrs\Event\MetaData;
use Gica\Cqrs\EventStore\Mongo\EventSerializer;
use Gica\Cqrs\EventStore\Mongo\MongoEventStore;
use Gica\Lib\ObjectToArrayConverter;
use Gica\Types\Guid;
use tests\Gica\Cqrs\MongoTestHelper;

class MongoEventStoreTest extends \PHPUnit_Framework_TestCase
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

        $eventStore = new MongoEventStore(
            $collection,
            new EventSerializer(),
            new ObjectToArrayConverter());

        $eventStore->dropStore();
        $eventStore->createStore();

        $aggregateId = 123;
        $aggregateClass = 'aggClass';

        $events = $this->wrapEventsWithMetadata($aggregateClass, $aggregateId, [new Event1(11), new Event2(22)]);

        $eventStore->appendEventsForAggregate($aggregateId, $aggregateClass, $events, -1, 0);

        $this->assertCount(1, $collection->find()->toArray());

        $stream = $eventStore->loadEventsForAggregate($aggregateClass, $aggregateId);

        $events = iterator_to_array($stream->getIterator());

        $this->assertCount(2, $events);

        $this->assertInstanceOf(Event1::class, $events[0]->getEvent());
        $this->assertInstanceOf(Event2::class, $events[1]->getEvent());
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
     * @expectedException \Gica\Cqrs\EventStore\Exception\ConcurrentModificationException
     */
    public function test_appendEventsForAggregateShouldNotWriteTwiceTheSameEvents()
    {
        $collection = $this->collection;

        $eventStore = new MongoEventStore(
            $collection,
            new EventSerializer(),
            new ObjectToArrayConverter());

        $eventStore->dropStore();
        $eventStore->createStore();

        $aggregateId = 123;

        $events = $this->wrapEventsWithMetadata($aggregateId, 'aggClass', [new Event1(11), new Event2(22)]);

        $eventStore->appendEventsForAggregate($aggregateId, 'aggClass', $events, 0, 0);

        $eventStore->appendEventsForAggregate($aggregateId, 'aggClass', $events, 0, 0);//should fail
    }
}

class Event1 implements \Gica\Cqrs\Event
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

class Event2 implements \Gica\Cqrs\Event
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
