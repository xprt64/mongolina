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
    private $aggregateVersion = [];

    public function __construct(private Collection $collection, private ObjectSerializer $objectSerializer, private ObjectHydrator $objectHydrator)
    {
    }

    private function factoryHydrator(string $entityClass): callable
    {
        return fn($document) => $this->objectHydrator->hydrateObject($entityClass, $document['entity']);
    }

    #[\Override]
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

    #[\Override]
    public function saveAggregate($aggregateId, $aggregate, $newEventsWithMeta)
    {
        if (!$newEventsWithMeta) {
            return [];
        }
        $aggregateDescriptor = new AggregateDescriptor($aggregateId, $aggregate::class);

        $serialized = $this->objectSerializer->convert($aggregate);
        unset($serialized['version']);

        $version = $this->getStoredAggregateVersion($aggregateDescriptor);
        $newEventsWithMeta = array_map(fn(EventWithMetaData $eventWithMetaData) => $eventWithMetaData->withVersion($version + 1), $newEventsWithMeta);
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

        return array_map(fn(EventWithMetaData $event) => $event->withVersion($version + 1), $newEventsWithMeta);
    }

    /**
     * @param EventWithMetaData[] $events
     * @return string[]
     */
    private function getEventClasses($events): array
    {
        return array_map(fn(EventWithMetaData $eventWithMetaData) => $eventWithMetaData->getEvent()::class, $events);
    }

    private function rehydrateAggregate(string $aggregateClass, ?array $document)
    {
        return \call_user_func($this->factoryHydrator($aggregateClass), $document);
    }

    public function loadEventsByClassNames(array $eventClasses): \Dudulina\EventStore\EventStream
    {
        return new CqrsAllEventByClassesStream($this->collection, $eventClasses, fn($document) => new EventWithMetaData(
            $this->objectHydrator->hydrateObject($document['events']['@classes']['event'], $document['events']['event']),
            $this->objectHydrator->hydrateObject($document['events']['@classes']['metaData'], $document['events']['metaData'])
        ));
    }

    public function create()
    {
        $this->collection->createIndex(['events.@classes.event' => 1]);
    }
}