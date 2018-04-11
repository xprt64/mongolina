<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;

use Dudulina\Command\CommandMetadata;
use Dudulina\Event\EventWithMetaData;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\UTCDateTime;

class EventsCommit
{
    /** @var ObjectID */
    private $streamName;

    /** @var string */
    private $aggregateId;

    /** @var string */
    private $aggregateClass;

    /** @var int */
    private $version;

    /** @var Timestamp */
    private $ts;

    /** @var UTCDateTime */
    private $createdAt;

    /** @var string|null */
    private $authenticatedUserId;

    /** @var CommandMetadata|null */
    private $commandMeta;

    /** @var EventWithMetaData[] */
    private $eventsWithMetadata;

    public function __construct(
        ObjectID $streamName,
        string $aggregateId,
        string $aggregateClass,
        int $version,
        Timestamp $ts,
        UTCDateTime $createdAt,
        ?string $authenticatedUserId,
        ?CommandMetadata $commandMeta,
        array $eventsWithMetadata)
    {
        $this->streamName = $streamName;
        $this->aggregateId = $aggregateId;
        $this->aggregateClass = $aggregateClass;
        $this->version = $version;
        $this->ts = $ts;
        $this->createdAt = $createdAt;
        $this->authenticatedUserId = $authenticatedUserId;
        $this->commandMeta = $commandMeta;
        $this->eventsWithMetadata = $eventsWithMetadata;
    }

    public function getStreamName(): ObjectID
    {
        return $this->streamName;
    }

    public function getAggregateId(): string
    {
        return $this->aggregateId;
    }

    public function getAggregateClass(): string
    {
        return $this->aggregateClass;
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    public function getTs(): Timestamp
    {
        return $this->ts;
    }

    public function getCreatedAt(): UTCDateTime
    {
        return $this->createdAt;
    }

    public function getAuthenticatedUserId():?string
    {
        return $this->authenticatedUserId;
    }

    public function getCommandMeta():?CommandMetadata
    {
        return $this->commandMeta;
    }

    /**
     * @return EventWithMetaData[]
     */
    public function getEventsWithMetadata(): array
    {
        return $this->eventsWithMetadata;
    }
}