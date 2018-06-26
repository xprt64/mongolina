<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

declare(strict_types=1);

namespace Mongolina;

use Dudulina\Aggregate\AggregateDescriptor;
use Dudulina\Event\EventWithMetaData;
use Dudulina\EventProcessing\ConcurentEventProcessingException;
use Gica\Serialize\ObjectHydrator\ObjectHydrator;
use Gica\Serialize\ObjectSerializer\ObjectSerializer;
use MongoDB\BSON\ObjectId;
use MongoDB\Collection;

class CqrsMongoAggregateRepository implements \Dudulina\Aggregate\AggregateRepository
{
    /**
     * @var Collection
     */
    private $collection;

    private $aggregateVersion = [];
    /**
     * @var ObjectSerializer
     */
    private $objectSerializer;
    /**
     * @var ObjectHydrator
     */
    private $objectHydrator;

    public function __construct(
        Collection $collection,
        ObjectSerializer $objectSerializer,
        ObjectHydrator $objectHydrator
    )
    {
        $this->collection = $collection;
        $this->objectSerializer = $objectSerializer;
        $this->objectHydrator = $objectHydrator;
    }

    private function factoryHydrator(string $entityClass): callable
    {
        return function ($document) use ($entityClass) {
            return $this->objectHydrator->hydrateObject($entityClass, $document['entity']);
        };
    }

    public function loadAggregate(AggregateDescriptor $aggregateDescriptor)
    {
        $document = $this->collection->findOne([
            '_id' => \Mongolina\StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor),
        ]);

        $aggregateClass = $aggregateDescriptor->getAggregateClass();
        if ($document) {
            $entity = $this->rehydrateAggregate($aggregateClass, $document);
            $version = $document['version'];
        } else {
            $entity = new $aggregateClass;
            $version = 0;
        }

        $this->storeAggregateVersion($aggregateDescriptor, $version);
        return $entity;
    }

    private function storeAggregateVersion(AggregateDescriptor $aggregateDescriptor, int $version): void
    {
        $this->aggregateVersion[$aggregateDescriptor->getAggregateClass() . '-' . $aggregateDescriptor->getAggregateId()] = $version;
    }

    private function getStoredAggregateVersion(AggregateDescriptor $aggregateDescriptor): int
    {
        return $this->aggregateVersion[$aggregateDescriptor->getAggregateClass() . '-' . $aggregateDescriptor->getAggregateId()];
    }

    public function saveAggregate($aggregateId, $aggregate, $newEventsWithMeta)
    {
        if (!$newEventsWithMeta) {
            return [];
        }
        $aggregateDescriptor = new AggregateDescriptor($aggregateId, \get_class($aggregate));

        $serialized = $this->objectSerializer->convert($aggregate);
        unset($serialized['version']);
        try {
            $version = $this->getStoredAggregateVersion($aggregateDescriptor);
            $newEventsWithMeta = array_map(function (EventWithMetaData $eventWithMetaData) use ($version) {
                return $eventWithMetaData->withVersion($version + 1);
            }, $newEventsWithMeta);
            $result = $this->collection->updateOne(
                [
                    '_id'     => \Mongolina\StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor),
                    'version' => $version,
                ],
                [
                    '$set'  => ['entity' => $serialized, 'aggregate' => ['id' => new ObjectId((string)$aggregateId), 'class' => $aggregateDescriptor->getAggregateClass()]],
                    '$inc'  => ['version' => 1],
                    '$push' => ['events' => ['$each' => $this->objectSerializer->convert($newEventsWithMeta)]],
                ],
                [
                    'upsert' => true,
                ]
            );
            $this->collection->updateOne(
                [
                    '_id'     => \Mongolina\StreamName::factoryStreamNameFromDescriptor($aggregateDescriptor),
                    'version' => $version + 1,
                ],
                [
                    '$pull' => [
                        'events' => [
                            '@classes.event'   => ['$in' => $this->getEventClasses($newEventsWithMeta)],
                            'metaData.version' => ['$lte' => $version],
                        ],
                    ],
                ],
                [
                    'upsert' => false,
                ]
            );
        } catch (\MongoDB\Driver\Exception\WriteException $writeException) {
            $result = $writeException->getWriteResult();
        }

        if (0 == $result->getMatchedCount() && 0 == $result->getUpsertedCount()) {//no side effect? then concurrent update -> retry
            throw new ConcurentEventProcessingException("");
        }

        return array_map(function (EventWithMetaData $event) use ($version) {
            return $event->withVersion($version + 1);
        }, $newEventsWithMeta);
    }

    /**
     * @param EventWithMetaData[] $events
     * @return string[]
     */
    private function getEventClasses($events): array
    {
        return array_map(function (EventWithMetaData $eventWithMetaData) {
            return \get_class($eventWithMetaData->getEvent());
        }, $events);
    }

    private function rehydrateAggregate(string $aggregateClass, ?array $document)
    {
        return \call_user_func($this->factoryHydrator($aggregateClass), $document);
    }

    public function loadEventsByClassNames(array $eventClasses): \Dudulina\EventStore\EventStream
    {
        return new CqrsAllEventByClassesStream($this->collection, $eventClasses, function ($document) {
            return new EventWithMetaData(
                $this->objectHydrator->hydrateObject($document['events']['@classes']['event'], $document['events']['event']),
                $this->objectHydrator->hydrateObject($document['events']['@classes']['metaData'], $document['events']['metaData'])
            );
        });
    }

    public function create()
    {
        $this->collection->createIndex(['events.@classes.event' => 1]);
    }
}