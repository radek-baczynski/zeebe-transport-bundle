<?php


namespace ZeebeTransportBundle\Messenger\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Serializer\SerializerInterface;
use ZeebeTransportBundle\Messenger\Message\ZeebeCreateInstanceStamp;
use ZeebeTransportBundle\Messenger\Message\ZeebePublishMessageStamp;
use ZeebeTransportBundle\Messenger\Transport\Exception\ZeebeException;

class ZeebeSender implements SenderInterface
{
    /** @var ZeebeConnection */
    private $connection;
    /**
     * @var SerializerInterface
     */
    private $serializer;

    public function __construct(ZeebeConnection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
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

    /**
     * @param  Envelope  $envelope
     *
     * @return Envelope
     * @throws ZeebeException|\Exception
     */
    private function sendMessage(Envelope $envelope)
    {
        $message = $envelope->getMessage();

        /** @var ZeebePublishMessageStamp $stamp */
        $stamp = $envelope->last(ZeebePublishMessageStamp::class);

        $payload = $this->serializer->serialize($message, 'json');

        $messageId = $this->generateId();

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

    /**
     * @return string
     * @throws \Exception
     */
    private function generateId(): string
    {
        return bin2hex(random_bytes(16));
    }
}
