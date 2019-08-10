<?php

namespace ZeebeTransportBundle\Messenger\Transport;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class TransportFactory implements TransportFactoryInterface
{
    /** @var \Symfony\Component\Serializer\SerializerInterface */
    private $symfonySerializer;

    public function __construct(\Symfony\Component\Serializer\SerializerInterface $symfonySerializer)
    {
        $this->symfonySerializer = $symfonySerializer;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $dsn = parse_url($dsn);

        return new Transport(
            new Connection($dsn['host'], (int)$dsn['port']),
            $this->symfonySerializer,
            $options
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return 0 === strpos($dsn, 'zeebe://');
    }
}
