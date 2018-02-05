<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\EventProcessing;


use Dudulina\EventProcessing\InProgressProcessingEvent;

class MongoInProgressProcessingEvent implements InProgressProcessingEvent
{

    /**
     * @var \DateTimeImmutable
     */
    private $date;
    /**
     * @var
     */
    private $eventId;

    public function __construct(
        \DateTimeImmutable $date,
        $eventId
    )
    {
        $this->date = $date;
        $this->eventId = $eventId;
    }

    public function getDate(): \DateTimeImmutable
    {
        return $this->date;
    }

    public function getEventId()
    {
        return $this->eventId;
    }
}