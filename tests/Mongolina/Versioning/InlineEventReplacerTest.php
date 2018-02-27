<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace tests\Dudulina\EventStore\Mongo\Versioning\InlineEventMigratorTest;

require_once __DIR__ . '/../MongoTestHelper.php';

use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\Event\MetaData;
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

class InlineEventReplacerTest extends \PHPUnit_Framework_TestCase
{
    const AGGREGATE_CLASS = 'aggClass';
    const AGGREGATE_ID    = 123;
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

        $expectedEventStream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor());

        $events = $this->wrapEventsWithMetadata([new Event1(11), new Event2(22)]);

        $eventStore->appendEventsForAggregate($this->factoryAggregateDescriptor(), $events, $expectedEventStream);

        $sut = new InlineEventReplacer(
            $this->collection,
            new CommitSerializer(
                new EventSerializer(),
                new ObjectToArrayConverter()
            )
        );

        $sut->replaceEvent(new class implements EventReplacer
        {
            public function replaceEvent(EventWithMetaData $eventWithMetaData): EventWithMetaData
            {
                $event = $eventWithMetaData->getEvent();

                if (!$event instanceof Event2) {
                    throw new \InvalidArgumentException("Wrong event  class: " . \get_class($event));
                }

                return new EventWithMetaData(
                    new Event3(100),
                    $eventWithMetaData->getMetaData()
                );
            }
        }, Event2::class);

        $this->assertCount(1, $this->collection->find()->toArray());//still 1 commit

        $stream = $eventStore->loadEventsForAggregate($this->factoryAggregateDescriptor());

        /** @var EventWithMetaData[] $events */
        $events = iterator_to_array($stream->getIterator(), false);

        $this->assertCount(2, $events); //stil 2 events

        $this->assertInstanceOf(Event1::class, $events[0]->getEvent());//the Event1 is the same
        $this->assertInstanceOf(Event3::class, $events[1]->getEvent());//the Event2 is replaced

        /** @var Event3 $event3 */
        $event3 = $events[1]->getEvent();

        $this->assertSame($event3->getField2(), 100);
    }

    private function factoryAggregateDescriptor(): AggregateDescriptor
    {
        return new AggregateDescriptor(123, self::AGGREGATE_CLASS);
    }

    private function wrapEventsWithMetadata($events)
    {
        return array_map(function ($event) {
            return $this->wrapEventWithMetadata(self::AGGREGATE_CLASS, self::AGGREGATE_ID, $event);
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
            new ObjectToArrayConverter()
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
