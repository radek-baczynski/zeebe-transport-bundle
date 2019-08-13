<?php


namespace ZeebeTransportBundle\Messenger\Message;


use Symfony\Component\Messenger\Stamp\StampInterface;
use ZeebeClient\ActivatedJob;

class ZeebeReceiveStamp implements StampInterface
{
    private $key = 0;
    private $type = '';
    private $workflowInstanceKey = 0;
    private $bpmnProcessId = '';
    private $workflowDefinitionVersion = 0;
    private $workflowKey = 0;
    private $elementId = '';
    private $elementInstanceKey = 0;
    private $customHeaders = [];
    private $worker = '';
    private $retries = 0;
    private $deadline = 0;
    private $variables = [];

    private function __construct(
        int $key,
        string $type,
        int $workflowInstanceKey,
        string $bpmnProcessId,
        int $workflowDefinitionVersion,
        int $workflowKey,
        string $elementId,
        int $elementInstanceKey,
        array $customHeaders,
        string $worker,
        int $retries,
        int $deadline,
        array $variables
    ) {
        $this->key                       = $key;
        $this->type                      = $type;
        $this->workflowInstanceKey       = $workflowInstanceKey;
        $this->bpmnProcessId             = $bpmnProcessId;
        $this->workflowDefinitionVersion = $workflowDefinitionVersion;
        $this->workflowKey               = $workflowKey;
        $this->elementId                 = $elementId;
        $this->elementInstanceKey        = $elementInstanceKey;
        $this->customHeaders             = $customHeaders;
        $this->worker                    = $worker;
        $this->retries                   = $retries;
        $this->deadline                  = $deadline;
        $this->variables                 = $variables;
    }

    public static function newFromActivatedJob(ActivatedJob $job): self
    {
        return new self(
            $job->getKey(),
            $job->getType(),
            $job->getWorkflowInstanceKey(),
            $job->getBpmnProcessId(),
            $job->getWorkflowDefinitionVersion(),
            $job->getWorkflowKey(),
            $job->getElementId(),
            $job->getElementInstanceKey(),
            json_decode($job->getCustomHeaders(), true) ?? [],
            $job->getWorker(),
            $job->getRetries(),
            $job->getDeadline(),
            json_decode($job->getVariables(), true) ?? []
        );
    }

    /**
     * @return int
     */
    public function getRetries(): int
    {
        return $this->retries;
    }

}
