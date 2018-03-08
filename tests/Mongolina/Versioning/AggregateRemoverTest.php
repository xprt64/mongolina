<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\EventStore\Mongo\Versioning\AggregateRemoverTest;

require_once __DIR__ . '/../MongoTestHelper.php';

use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\Event\MetaData;
use Gica\Serialize\ObjectSerializer\CompositeSerializer;
use Gica\Serialize\ObjectSerializer\ObjectSerializer;
use Mongolina\Versioning\AggregateRemover;
use Mongolina\Versioning\InlineEventReplacer;
use Mongolina\EventsCommit\CommitSerializer;
use Gica\Lib\ObjectToArrayConverter;
use Gica\Types\Guid;
use Mongolina\EventSerializer;
use Mongolina\EventStreamIterator;
use Mongolina\MongoAggregateAllEventStreamFactory;
use Mongolina\MongoAllEventByClassesStreamFactory;
use Mongolina\MongoEventStore;
use Mongolina\Versioning\InlineEventMigrator\EventReplacer;
use tests\Dudulina\MongoTestHelper;

class AggregateRemoverTest extends \PHPUnit_Framework_TestCase
{
    const AGGREGATE_CLASS = 'aggClass';
    const AGGREGATE_ID    = 123;
    const AGGREGATE_ID2    = 456;
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

        $eventStore->appendEventsForAggregate(
            $this->factoryAggregateDescriptor(),
            $this->wrapEventsWithMetadata([new Event1(11), new Event2(22)]),
            $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor()));

        $eventStore->appendEventsForAggregate(
            $this->factoryAggregateDescriptor2(),
            $this->wrapEventsWithMetadataForAggregate2([new Event1(33), new Event2(44)]),
            $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor2()));

        $this->assertCount(2, $this->collection->find()->toArray());

        $sut = new AggregateRemover($this->collection);

        $sut->removeAggregate($this->factoryAggregateDescriptor());

        $this->assertCount(1, $this->collection->find()->toArray());

        $stream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor2());

        /** @var EventWithMetaData[] $events */
        $events = iterator_to_array($stream->getIterator(), false);

        $this->assertCount(2, $events); //stil 2 events

        $this->assertInstanceOf(Event1::class, $events[0]->getEvent());
        $this->assertSame(33, $events[0]->getEvent()->getField1());
        $this->assertInstanceOf(Event2::class, $events[1]->getEvent());
        $this->assertSame(44, $events[1]->getEvent()->getField2());
   }

    private function factoryAggregateDescriptor(): AggregateDescriptor
    {
        return new AggregateDescriptor(self::AGGREGATE_ID, self::AGGREGATE_CLASS);
    }

    private function factoryAggregateDescriptor2(): AggregateDescriptor
    {
        return new AggregateDescriptor(self::AGGREGATE_ID2, self::AGGREGATE_CLASS);
    }

    private function wrapEventsWithMetadata($events)
    {
        return array_map(function ($event) {
            return $this->wrapEventWithMetadata(self::AGGREGATE_CLASS, self::AGGREGATE_ID, $event);
        }, $events);
    }

    private function wrapEventsWithMetadataForAggregate2($events)
    {
        return array_map(function ($event) {
            return $this->wrapEventWithMetadata(self::AGGREGATE_CLASS, self::AGGREGATE_ID2, $event);
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

    public function getField2()
    {
        return $this->field2;
    }
}

class Event3 implements \Dudulina\Event
{
    private $field2;

    public function __construct($field2)
    {
        $this->field2 = $field2;
    }

    public function getField2()
    {
        return $this->field2;
    }
}
