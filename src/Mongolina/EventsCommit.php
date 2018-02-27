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

    /** @var int */
    private $sequence;

    /** @var UTCDateTime */
    private $createdAt;

    /** @var string|null */
    private $authenticatedUserId;

    /** @var CommandMetadata|null */
    private $commandMeta;

    /** @var EventWithMetaData[] */
    private $eventsWithMetadata = [];

    public function __construct(
        ObjectID $streamName,
        string $aggregateId,
        string $aggregateClass,
        int $version,
        Timestamp $ts,
        int $sequence,
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
        $this->sequence = $sequence;
        $this->createdAt = $createdAt;
        $this->authenticatedUserId = $authenticatedUserId;
        $this->commandMeta = $commandMeta;
        $this->eventsWithMetadata = $eventsWithMetadata;
    }

    /**
     * @return ObjectID
     */
    public function getStreamName(): ObjectID
    {
        return $this->streamName;
    }

    /**
     * @return string
     */
    public function getAggregateId(): string
    {
        return $this->aggregateId;
    }

    /**
     * @return string
     */
    public function getAggregateClass(): string
    {
        return $this->aggregateClass;
    }

    /**
     * @return int
     */
    public function getVersion(): int
    {
        return $this->version;
    }

    /**
     * @return Timestamp
     */
    public function getTs(): Timestamp
    {
        return $this->ts;
    }

    /**
     * @return int
     */
    public function getSequence(): int
    {
        return $this->sequence;
    }

    /**
     * @return UTCDateTime
     */
    public function getCreatedAt(): UTCDateTime
    {
        return $this->createdAt;
    }

    /**
     * @return null|string
     */
    public function getAuthenticatedUserId()
    {
        return $this->authenticatedUserId;
    }

    /**
     * @return CommandMetadata|null
     */
    public function getCommandMeta()
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