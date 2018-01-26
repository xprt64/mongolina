<?php


namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Command\CommandMetadata;
use Gica\Cqrs\EventStore\Mongo\ScheduledCommand\ScheduledCommandStoreTrait;
use Gica\Cqrs\Scheduling\ScheduledCommand;
use MongoDB\BSON\UTCDateTime;

class CommandScheduler implements \Gica\Cqrs\Scheduling\CommandScheduler
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