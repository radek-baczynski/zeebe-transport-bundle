<?php


namespace ZeebeTransportBundle\Messenger\Message;


use Symfony\Component\Messenger\Stamp\StampInterface;

class ZeebeCreateInstanceStamp implements StampInterface
{
    private $workflowName;

    private $version;

    public function __construct(string $workflowName, int $version = -1)
    {
        $this->workflowName = $workflowName;
        $this->version      = $version;
    }

    public function getWorkflowName(): string
    {
        return $this->workflowName;
    }

    public function getVersion(): string
    {
        return $this->version;
    }
}
