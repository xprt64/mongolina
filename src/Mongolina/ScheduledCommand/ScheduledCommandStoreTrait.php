<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\ScheduledCommand;

use Gica\Types\Guid;
use MongoDB\BSON\ObjectID;
use MongoDB\Collection;

trait ScheduledCommandStoreTrait
{

    /* @var Collection */
    private $collection;

    public function __construct(
        Collection $collection
    )
    {
        $this->collection = $collection;
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
}