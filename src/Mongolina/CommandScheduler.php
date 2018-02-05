<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */
namespace Mongolina;

use Dudulina\Command\CommandMetadata;
use Mongolina\ScheduledCommand\ScheduledCommandStoreTrait;
use Dudulina\Scheduling\ScheduledCommand;
use MongoDB\BSON\UTCDateTime;

class CommandScheduler implements \Dudulina\Scheduling\CommandScheduler
{
    use ScheduledCommandStoreTrait;

    public function scheduleCommand(ScheduledCommand $scheduledCommand, string $aggregateClass, $aggregateId, CommandMetadata $commandMetadata = null)
    {
        $messageIdToMongoId = $this->messageIdToMongoId($scheduledCommand->getMessageId());

        $this->collection->updateOne([
            '_id' => $messageIdToMongoId,
        ], [
            '$set' => [
                '_id'             => $messageIdToMongoId,
                'scheduleAt'      => new UTCDateTime($scheduledCommand->getFireDate()->getTimestamp() * 1000),
                'command'         => \serialize($scheduledCommand),
                'commandMetadata' => $commandMetadata ? \serialize($commandMetadata) : null,
                'aggregate'       => [
                    'id'    => (string)$aggregateId,
                    'class' => $aggregateClass,
                ],
            ],
        ], [
            'upsert' => true,
        ]);
    }

    public function cancelCommand($commandId)
    {
        $this->collection->deleteOne([
            '_id' => $this->messageIdToMongoId($commandId),
        ]);
    }
}