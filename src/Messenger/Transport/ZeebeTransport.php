<?php


namespace ZeebeTransportBundle\Messenger\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
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

    /**
     * @var ZeebeReceiver
     */
    private $receiver;

    /**
     * @var ZeebeSender
     */
    private $sender;

    public function __construct(ZeebeConnection $connection, SerializerInterface $serializer, array $options = [])
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
        $this->options    = $options;

        $this->receiver = new ZeebeReceiver($connection, $serializer);
        $this->sender   = new ZeebeSender($connection, $serializer);
    }

    public function get(): iterable
    {
        return $this->receiver->get();
    }

    public function ack(Envelope $envelope): void
    {
        $this->receiver->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->receiver->reject($envelope);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->sender->send($envelope);
    }


}
