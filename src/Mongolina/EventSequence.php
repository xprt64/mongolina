<?php
/**
 * Copyright (c) 2018 Constantin Galbenu <xprt64@gmail.com>
 */

namespace Mongolina;


use MongoDB\BSON\Timestamp;

class EventSequence implements \Dudulina\EventStore\EventSequence
{

    /**
     * @var Timestamp
     */
    private $timestamp;
    /**
     * @var int
     */
    private $indexInsideCommit;

    public function __construct(
        Timestamp $timestamp,
        int $indexInsideCommit
    )
    {
        $this->timestamp = $timestamp;
        $this->indexInsideCommit = $indexInsideCommit;
    }

    public function getTimestamp(): Timestamp
    {
        return $this->timestamp;
    }

    public function getIndexInsideCommit(): int
    {
        return $this->indexInsideCommit;
    }

    public function __toString()
    {
        return $this->timestamp->__toString() . ';' . $this->indexInsideCommit;
    }

    public static function fromString(string $str): self
    {
        list($timestampStr, $indexStr) = explode(';', $str);
        if (!self::isTimestampString($timestampStr)) {
            throw new \InvalidArgumentException("Not a valid timestamp:$timestampStr");
        }
        return new static(self::factoryTimestampFromString($timestampStr), (int)$indexStr);
    }

    public static function isValidString(string $str): bool
    {
        list($timestampStr, $indexStr) = explode(';', $str);
        return self::isTimestampString($timestampStr) && is_numeric($indexStr);
    }

    public static function factoryTimestampFromString($value): Timestamp
    {
        list($a, $b) = explode(':', trim($value, '[]'));

        $a = $a < 0 ? 0 : $a;
        $b = $b < 0 ? 0 : $b;

        return new Timestamp((int)$a, (int)$b);
    }

    public static function isTimestampString($value): bool
    {
        return preg_match('#[\d+,\d+]#im', $value);
    }


    public function isBefore(\Dudulina\EventStore\EventSequence $other): bool
    {
        return $this->timestamp < $other->timestamp || ($this->timestamp === $other->timestamp && $this->indexInsideCommit < $other->indexInsideCommit);
    }

    public function isAfter(self $other): bool
    {
        return $this->timestamp > $other->timestamp || ($this->timestamp === $other->timestamp && $this->indexInsideCommit > $other->indexInsideCommit);
    }

}