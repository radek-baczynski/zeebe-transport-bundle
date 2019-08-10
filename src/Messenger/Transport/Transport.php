<?php


namespace ZeebeTransportBundle\Messenger\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Serializer\SerializerInterface;
use ZeebeTransportBundle\Messenger\Message\PublishableMessageInterface;
use ZeebeTransportBundle\Messenger\Message\ZeebeStamp;

class Transport implements TransportInterface
{
    private $connection;
    /**
     * @var SerializerInterface
     */
    private $serializer;
    /**
     * @var array
     */
    private $options;

    public function __construct(Connection $connection, SerializerInterface $serializer, array $options = [])
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
        $this->options    = $options;
    }

    /**
     * Receives some messages.
     *
     * While this method could return an unlimited number of messages,
     * the intention is that it returns only one, or a "small number"
     * of messages each time. This gives the user more flexibility:
     * they can finish processing the one (or "small number") of messages
     * from this receiver and move on to check other receivers for messages.
     * If this method returns too many messages, it could cause a
     * blocking effect where handling the messages received from one
     * call to get() takes a long time, blocking other receivers from
     * being called.
     *
     * If applicable, the Envelope should contain a TransportMessageIdStamp.
     *
     * If a received message cannot be decoded, the message should not
     * be retried again (e.g. if there's a queue, it should be removed)
     * and a MessageDecodingFailedException should be thrown.
     *
     * @return Envelope[]
     * @throws TransportException If there is an issue communicating with the transport
     *
     */
    public function get(): iterable
    {
        $envelopes = [];

        $activeJobs = $this->connection->getActiveJobs(
            $this->options['type'] ?? 'symfony_message',
            $this->options['worker_name'] ?? 'symfony_worker',
            $this->options['timeout'] ?? 60,
            $this->options['size'] ?? 1
        );

        foreach ($activeJobs as $job) {
            $headers = json_decode($job->getCustomHeaders(), true) ?? [];
            $message = $this->serializer->deserialize($job->getVariables() ?? '{}', $headers['message_class'], 'json');

            $envelope = new Envelope($message, [
                new TransportMessageIdStamp($job->getKey()),
                ZeebeStamp::newFromActivatedJob($job),
            ]);

            $envelopes[] = $envelope;
        }

        return $envelopes;
    }

    /**
     * Acknowledges that the passed message was handled.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function ack(Envelope $envelope): void
    {
        /** @var TransportMessageIdStamp $idStamp */
        $idStamp = $envelope->last(TransportMessageIdStamp::class);

        /** @var ZeebeStamp $zeebeStamp */
        $zeebeStamp = $envelope->last(ZeebeStamp::class);

        /** @var HandledStamp $handled */
        $handled = $envelope->last(HandledStamp::class);

        $payload = '';
        $result  = $handled->getResult();
        if ($result) {
            $payload = $this->serializer->serialize($result, 'json');
        }


        $this->connection->completeJob($idStamp->getId(), $payload);
    }

    /**
     * Called when handling the message failed and it should not be retried.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function reject(Envelope $envelope): void
    {
        /** @var TransportMessageIdStamp $idStamp */
        $idStamp = $envelope->last(TransportMessageIdStamp::class);

        $this->connection->failJob($idStamp->getId());
    }

    /**
     * Sends the given envelope.
     *
     * The sender can read different stamps for transport configuration,
     * like delivery delay.
     *
     * If applicable, the returned Envelope should contain a TransportMessageIdStamp.
     */
    public function send(Envelope $envelope): Envelope
    {
        if ($this->isInitWorkflowMessage($envelope)) {
            return $this->initWorkflow($envelope);
        }

        return $this->sendMessage($envelope);
    }

    private function isInitWorkflowMessage(Envelope $envelope): bool
    {
        $class = get_class($envelope->getMessage());

        return array_key_exists($class, $this->options['init-workflow'] ?? []);
    }

    private function initWorkflow(Envelope $envelope)
    {
        $messageClass = get_class($envelope->getMessage());

        $bpmnProcessId = $this->options['init-workflow'][$messageClass] ?? null;

        if (!$bpmnProcessId) {
            throw new \Exception("Message {$messageClass} has not mapped zeebe workflow bpmnProcessId");
        }

        $payload  = $this->serializer->serialize($envelope->getMessage(), 'json');
        $response = $this->connection->createWorkflow($bpmnProcessId, -1, $payload);

        if (!$response) {
            throw new \Exception('Workflow not created');
        }

        $stampId = new TransportMessageIdStamp($response->getWorkflowKey());

        return $envelope->with($stampId);
    }

    private function sendMessage(Envelope $envelope)
    {
        $message = $envelope->getMessage();

        if (!$message instanceof PublishableMessageInterface) {
            throw new \LogicException('Message must implement PublishableMessageInterface to be published on Zeebe');
        }

        $payload = $this->serializer->serialize($message, 'json');

        $response = $this->connection->sendMessage(
            $message->getName(),
            $message->getCorrelationKey(),
            $this->options['publish']['time_to_live'] ?? 100,
            $payload
        );

        $stampId = new TransportMessageIdStamp();

        return $envelope->with($stampId);
    }
}
