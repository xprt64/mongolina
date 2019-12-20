<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\ScheduledCommand;


use Dudulina\Scheduling\ScheduledCommand;

class ScheduledCommandWithMetadata
{

    /**
     * @var ScheduledCommand
     */
    private $scheduledCommand;
    /**
     * @var array|null
     */
    private $commandMetadata;

    public function __construct(
        ScheduledCommand $scheduledCommand,
        ?array $commandMetadata
    )
    {
        $this->scheduledCommand = $scheduledCommand;
        $this->commandMetadata = $commandMetadata;
    }

    public function getScheduledCommand(): ScheduledCommand
    {
        return $this->scheduledCommand;
    }

    public function getCommandMetadata():?array
    {
        return $this->commandMetadata;
    }
}