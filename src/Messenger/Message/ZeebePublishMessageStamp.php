<?php


namespace ZeebeTransportBundle\Messenger\Message;


use Symfony\Component\Messenger\Stamp\StampInterface;

class ZeebePublishMessageStamp implements StampInterface
{
    private $name;
    private $correlationKey;
    private $timeToLive;

    public function __construct(string $name, $correlationKey, int $timeToLive = 1000)
    {
        $this->name           = $name;
        $this->correlationKey = $correlationKey;
        $this->timeToLive     = $timeToLive;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getCorrelationKey()
    {
        return $this->correlationKey;
    }

    public function getTimeToLive(): int
    {
        return $this->timeToLive;
    }


}
