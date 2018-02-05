<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\Event\MetaData;
use MongoDB\BSON\UTCDateTime;

trait DocumentParserTrait
{
    private function extractMetaDataFromDocument($document)
    {
        /** @var UTCDateTime $createdAt */
        $createdAt = $document['createdAt'];
        $dateCreated = \DateTimeImmutable::createFromMutable($createdAt->toDateTime());

        return new MetaData(
            (string)$document['aggregateId'],
            $document['aggregateClass'],
            $dateCreated,
            $document['authenticatedUserId'] ? $document['authenticatedUserId'] : null
        );
    }

    private function extractSequenceFromDocument($document)
    {
        return $document['sequence'];
    }

    private function extractVersionFromDocument($document)
    {
        return $document['version'];
    }
}