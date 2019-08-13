<?php


namespace ZeebeTransportBundle\Messenger\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Serializer\SerializerInterface;
use ZeebeTransportBundle\Messenger\Message\ZeebeCreateInstanceStamp;
use ZeebeTransportBundle\Messenger\Message\ZeebePublishMessageStamp;
use ZeebeTransportBundle\Messenger\Message\ZeebeReceiveStamp;
use ZeebeTransportBundle\Messenger\Transport\Exception\ZeebeException;

class ZeebeTransport implements TransportInterface
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

    public function __construct(ZeebeConnection $connection, SerializerInterface $serializer, array $options = [])
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
        $this->options    = $options;
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
        if (null !== $envelope->last(ZeebeCreateInstanceStamp::class)) {
            return $this->initWorkflow($envelope);
        } elseif (null !== $envelope->last(ZeebePublishMessageStamp::class)) {
            return $this->sendMessage($envelope);
        }

        throw new ZeebeException('Publishing on zeebe requires ZeebeCreateInstanceStamp or ZeebePublishMessageStamp');
    }

    private function initWorkflow(Envelope $envelope)
    {
        /** @var ZeebeCreateInstanceStamp $stamp */
        $stamp         = $envelope->last(ZeebeCreateInstanceStamp::class);
        $bpmnProcessId = $stamp->getWorkflowName();

        $messageClass = get_class($envelope->getMessage());

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

        /** @var ZeebePublishMessageStamp $stamp */
        $stamp = $envelope->last(ZeebePublishMessageStamp::class);

        $payload = $this->serializer->serialize($message, 'json');

        $messageId = bin2hex(random_bytes(16));

        $this->connection->sendMessage(
            $stamp->getName(),
            $stamp->getCorrelationKey(),
            $stamp->getTimeToLive(),
            $messageId,
            $payload
        );

        $stampId = new TransportMessageIdStamp($messageId);

        return $envelope->with($stampId);
    }
}
