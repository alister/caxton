<?php

namespace App\Command;

use App\SampleGraphBuilder;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class CreateGraphCommand extends Command
{
    protected static $defaultName = 'app:create-graph';

    /** @var \App\SampleGraphBuilder */
    private $testGraph;

    public function __construct(SampleGraphBuilder $testGraph, $name = null)
    {
        parent::__construct($name); // MUST call constructor

        $this->testGraph = $testGraph;
    }

    protected function configure()
    {
        $this
            ->setDescription('Add a short description for your command')
            ->addArgument('qty', InputArgument::OPTIONAL, 'Total number of Persons to create')
            ->addOption('prime', null, InputOption::VALUE_REQUIRED, "Number of 'prime' Persons to create (must be less then :qty). These will connect to others")
        ;
    }
    
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $io = new SymfonyStyle($input, $output);
        $qty = (int) $input->getArgument('qty');

        if ($qty) {
            #$io->note(sprintf('You passed an argument: %s', $qty));
        }

        $prime = $input->getOption('prime');
        if (!$prime) {
            $prime = (int) ($qty / 100);
            $prime = max(3, $prime);    // always provide at least 3
        }

        $this->testGraph->make($qty, $prime, $output);
    }
}
