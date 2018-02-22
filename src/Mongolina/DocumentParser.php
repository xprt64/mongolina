<?php
/**
 * Copyright (c) 2017 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use Dudulina\Event\MetaData;
use MongoDB\BSON\UTCDateTime;

class DocumentParser
{
    public function extractMetaDataFromDocument($document)
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

    public function extractSequenceFromDocument($document)
    {
        return $document['sequence'];
    }

    public function extractVersionFromDocument($document)
    {
        return $document['version'];
    }
}