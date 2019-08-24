<?php


namespace ZeebeTransportBundle\Command;


use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use ZeebeTransportBundle\Messenger\Transport\ZeebeConnection;
use ZeebeTransportBundle\Messenger\Transport\ZeebeTransportFactory;

class WorkflowDeployCommand extends Command
{
    public function __construct()
    {
        parent::__construct('zeebe:workflow:deploy');
    }

    protected function configure()
    {
        $this->addArgument('dsn', InputArgument::REQUIRED);
        $this->addArgument('file', InputArgument::REQUIRED);
    }


    protected function execute(InputInterface $input, OutputInterface $output)
    {
        [$host, $port] = explode(':', $input->getArgument('dsn'));

        $connection = new ZeebeConnection($host, (int)$port);

        $pathinfo = pathinfo($input->getArgument('file'));

        $type = $pathinfo['extension'];
        $name = $pathinfo['basename'];

        $connection->deployWorkflow($name, $type, file_get_contents($input->getArgument('file')));
    }


}
