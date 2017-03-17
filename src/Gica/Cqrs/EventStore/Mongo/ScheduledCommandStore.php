<?php


namespace Gica\Cqrs\EventStore\Mongo;


use Gica\Cqrs\Scheduling\ScheduledCommand;
use Gica\Types\Guid;
use MongoDB\BSON\ObjectID;
use MongoDB\BSON\UTCDateTime;

class ScheduledCommandStore implements \Gica\Cqrs\ScheduledCommandStore
{

    /* @var \MongoDB\Collection */
    private $collection;

    public function __construct(
        \MongoDB\Collection $collection
    )
    {
        $this->collection = $collection;
    }

    public function loadAndProcessScheduledCommands(callable $eventProcessor/** function(ScheduledCommand $scheduledCommand) */)
    {
        while (($command = $this->loadOneCommand())) {
            call_user_func($eventProcessor, $command);
        }
    }

    private function loadOneCommand()
    {
        $document = $this->collection->findOneAndDelete([
            'scheduleAt' => [
                '$lte' => new UTCDateTime(time() * 1000),
            ],
        ], [
            'sort' => ['scheduleAt' => 1],
        ]);

        if (!$document) {
            return null;
        }

        return $this->hydrateCommand($document);
    }

    private function hydrateCommand($document)
    {
        return unserialize($document['command']);
    }

    /**
     * @param ScheduledCommand[] $scheduledCommands
     */
    public function scheduleCommands($scheduledCommands)
    {
        foreach ($scheduledCommands as $command) {
            $this->scheduleCommand($command);
        }
    }

    public function scheduleCommand(ScheduledCommand $scheduledCommand)
    {
        $messageIdToMongoId = $this->messageIdToMongoId($scheduledCommand->getMessageId());

        $this->collection->updateOne([
            '_id' => $messageIdToMongoId,
        ], [
            '$set' => [
                '_id'        => $messageIdToMongoId,
                'scheduleAt' => new UTCDateTime($scheduledCommand->getFireDate()->getTimestamp() * 1000),
                'command'    => \serialize($scheduledCommand),
            ],
        ], [
            'upsert' => true,
        ]);
    }

    private function messageIdToMongoId($messageId): ObjectID
    {
        if (null === $messageId || '' === $messageId) {
            return new ObjectID(Guid::generate());
        }

        return new ObjectID(Guid::fromFixedString('scheduled-message-' . $messageId));
    }

    public function createStore()
    {
        $this->collection->createIndex(['scheduleAt' => 1, 'version' => 1]);
    }

    public function dropStore()
    {
        $this->collection->drop();
    }

    public function cancelCommand($commandId)
    {
        $messageIdToMongoId = $this->messageIdToMongoId($commandId);

        $this->collection->deleteOne([
            '_id' => $messageIdToMongoId,
        ]);
    }
}