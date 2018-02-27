<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina\Versioning\InlineEventMigrator;


use Dudulina\Event\EventWithMetaData;

interface EventReplacer
{
    public function replaceEvent(EventWithMetaData $eventWithMetaData): EventWithMetaData;
}