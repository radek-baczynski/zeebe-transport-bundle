<?php


namespace ZeebeTransportBundle\Messenger\Transport;

use Grpc\ChannelCredentials;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use ZeebeClient\ActivatedJob;
use ZeebeClient\ActivateJobsRequest;
use ZeebeClient\ActivateJobsResponse;
use ZeebeClient\CompleteJobRequest;
use ZeebeClient\CompleteJobResponse;
use ZeebeClient\CreateWorkflowInstanceRequest;
use ZeebeClient\CreateWorkflowInstanceResponse;
use ZeebeClient\FailJobRequest;
use ZeebeClient\GatewayClient;
use ZeebeClient\PublishMessageRequest;
use ZeebeClient\PublishMessageResponse;
use ZeebeTransportBundle\Messenger\Transport\Exception\ZeebeException;

class Connection
{
    /** @var GatewayClient */
    private $client;

    /**
     * Connection constructor.
     */
    public function __construct(string $host, int $port, ?string $tls = null)
    {
        $this->client = new GatewayClient("{$host}:{$port}", [
            'credentials' => $tls ?: ChannelCredentials::createInsecure(),
        ]);
    }

    /**
     * @param  string  $type
     * @param  string  $workerName
     * @param  int  $timeout
     * @param  int  $size
     *
     * @return ActivatedJob[]
     */
    public function getActiveJobs(string $type, string $workerName, int $timeout, int $size)
    {
        $request = new ActivateJobsRequest([
            'type'              => $type,
            'worker'            => $workerName,
            'timeout'           => $timeout,
            'maxJobsToActivate' => $size,
        ]);

        $jobs = [];

        $responses = $this->client->ActivateJobs($request)->responses();

        /** @var ActivateJobsResponse $response */
        foreach ($responses as $response) {
            /** @var ActivatedJob $job */
            foreach ($response->getJobs() as $job) {
                $jobs[] = $job;
            }
        }

        return $jobs;
    }

    public function completeJob($key, $payload): CompleteJobResponse
    {
        $completeRequest = new CompleteJobRequest([
            'jobKey'    => $key,
            'variables' => $payload,
        ]);

        [$rsp, $status] = $this->client->CompleteJob($completeRequest)->wait();

        if ($status->code != 0) {
            throw new ZeebeException($status->details, $status->code);
        }

        return $rsp;
    }

    public function failJob(string $key)
    {
        $failRequest = new FailJobRequest([
            'jobKey' => $key,

        ]);

        [$rsp, $status] = $this->client->FailJob($failRequest)->wait();
    }

    public function createWorkflow($bpmnProcessId, int $version, string $payload): ?CreateWorkflowInstanceResponse
    {
        $createRequest = new CreateWorkflowInstanceRequest([
            'bpmnProcessId' => $bpmnProcessId,
            'version'       => $version,
            'variables'     => $payload,
        ]);

        [$rsp, $status] = $this->client->CreateWorkflowInstance($createRequest)->wait();

        return $rsp;
    }

    public function sendMessage(
        string $name,
        string $correlationKey,
        int $timeToLive,
        string $variables
    ): PublishMessageResponse {
        $messageRequest = new PublishMessageRequest([
            'name'           => $name,
            'correlationKey' => $correlationKey,
            'timeToLive'     => $timeToLive,
            'variables'      => $variables,
        ]);

        [$rsp, $status] = $this->client->PublishMessage($messageRequest)->wait();

        if ($status->code != 0) {
            throw new ZeebeException($status->details, $status->code);
        }

        return $rsp;
    }
}