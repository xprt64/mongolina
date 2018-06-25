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
            '_id' => new ObjectId((string)$aggregateDescriptor->getAggregateId()),
        ]);

        $aggregateClass = $aggregateDescriptor->getAggregateClass();
        if ($document) {
            $entity = \call_user_func($this->factoryHydrator($aggregateClass), $document);
            $version = $document['version'];
        } else {
            $entity = new $aggregateClass;
            $version = 0;
        }

        $this->storeAggregateVersion($aggregateClass, $aggregateDescriptor->getAggregateId(), $version);
        return $entity;
    }

    private function storeAggregateVersion($aggregateClass, $aggregateId, int $version): void
    {
        $this->aggregateVersion[$aggregateClass . '-' . $aggregateId] = $version;
    }

    private function getStoredAggregateVersion($aggregateClass, $aggregateId): int
    {
        return $this->aggregateVersion[$aggregateClass . '-' . $aggregateId];
    }

    public function saveAggregate($aggregateId, $aggregate, $newEventsWithMeta)
    {
        $serialized = $this->objectSerializer->convert($aggregate);
        unset($serialized['version']);
        try {
            $version = $this->getStoredAggregateVersion(\get_class($aggregate), $aggregateId);
            $newEventsWithMeta = array_map(function(EventWithMetaData $eventWithMetaData) use ($version){
                return $eventWithMetaData->withVersion($version + 1);
            },$newEventsWithMeta);
            $result = $this->collection->updateOne(
                [
                    '_id'     => new ObjectID((string)$aggregateId),
                    'version' => $version,
                ],
                [
                    '$set'  => ['entity' => $serialized],
                    '$inc'  => ['version' => 1],
                    '$push' => ['events' => ['$each' => $this->objectSerializer->convert($newEventsWithMeta)]],
                ],
                [
                    'upsert' => true,
                ]
            );
            $this->collection->updateOne(
                [
                    '_id'     => new ObjectID((string)$aggregateId),
                    'version' => $version + 1,
                ],
                [
                    '$pull' => ['events' => [
                        '@classes.event' => ['$in' => $this->getEventClasses($newEventsWithMeta)],
                        'metaData.version' => ['$lte' => $version]
                    ]],
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
}