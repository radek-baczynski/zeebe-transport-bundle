<?php


namespace ZeebeTransportBundle\Messenger\Message;


interface PublishableMessageInterface
{
    public function getCorrelationKey();

    public function getName();
}
