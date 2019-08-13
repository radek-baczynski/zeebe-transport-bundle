<?php


namespace ZeebeTransportBundle\Messenger\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use ZeebeTransportBundle\Messenger\Message\ZeebeReceiveStamp;
use ZeebeTransportBundle\Messenger\Transport\Exception\ZeebeException;

class ZeebeReceiver implements ReceiverInterface
{

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
                ZeebeReceiveStamp::newFromActivatedJob($job),
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

        /** @var ZeebeReceiveStamp $zeebeStamp */
        $zeebeStamp = $envelope->last(ZeebeReceiveStamp::class);

        /** @var HandledStamp $handled */
        $handled = $envelope->last(HandledStamp::class);

        $payload = '';
        $result  = $handled->getResult();
        if ($result) {
            $payload = $this->serializer->serialize($result, 'json');
        }

        try {
            $this->connection->completeJob($idStamp->getId(), $payload);
        } catch (ZeebeException $exception) {
            throw new TransportException($exception->getMessage(), $exception->getCode(), $exception);
        }
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
        /** @var ZeebeReceiveStamp $zeebeStamp */
        $zeebeStamp = $envelope->last(ZeebeReceiveStamp::class);

        $retries = $zeebeStamp ? $zeebeStamp->getRetries() - 1 : 0;

        try {
            $this->connection->failJob(
                $idStamp->getId(),
                $retries
            );
        } catch (ZeebeException $exception) {
            throw new TransportException($exception->getMessage(), $exception->getCode(), $exception);
        }
    }
}
