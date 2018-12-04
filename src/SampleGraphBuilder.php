<?php

namespace App;

use Faker;
use Redis;
use Redis\Graph;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Helper\TableStyle;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

class SampleGraphBuilder
{
    private const DB_NUM = 10;

    /** @var \Redis */
    private $redis;
    /** @var \Faker\Generator */
    private $faker;
    /** @var \Symfony\Component\Stopwatch\Stopwatch */
    private $stopwatch;

    /** @var array */
    private $persons;
    /** @var array */
    private $primePersons;
    /** @var array */
    private $linkPersons;

    public function __construct()
    {
        $this->redis = new Redis();
        $this->redis->pconnect('localhost');
        $this->redis->select(self::DB_NUM);

        // don't write logs while we are bulk-inserting
        $this->redis->config('SET', 'SAVE', '1800 1');  
        $this->faker = Faker\Factory::create();
        $this->stopwatch = new Stopwatch(true);
    }

    public function __destruct()
    {
        $this->redis->ping();
        // reset to normal
        $this->redis->config('SET', 'SAVE', '900 1 300 10 60 10000');
    }

    public function make(int $qty, int $primeQty, OutputInterface $output): void
    {
        $this->deleteGraph();
        $graph = new Graph('social', $this->redis);

        list($totalConnections) = $this->generatePersonsAndLinks($qty, $primeQty);

        $this->buildAllPersons($output, $graph);
        // $this->showAllPersons($graph);
        $this->buildLinksBetweenPersons($output, $totalConnections, $graph);

        $this->printCountOfLinksPerPerson($graph);
        $this->showAllConnectionsForRandomPrimePerson($graph);

        $this->benchmarkGetLinksWithQuery($graph);
        
        dump($this->redis->info('memory'));
        // All done, remove graph from Redis memory.
        #$this->deleteGraph();

        $this->displaySummary($output, $totalConnections);
    }

    private function buildPersons(int $qty, int $primeQty): ?\Generator
    {
        $usernames = [];
        $primeCount = 0;
        
        for ($i = 0; $i < $qty; $i ++) {
            do {
                $username = $this->faker->userName;
            } while (isset($usernames[$username]));
            $usernames[$username] = 1;
            
            $chanceOfSetDateNotZero = $this->faker->boolean(30);
            $person = [
                'username' => $username,
                'name' => htmlentities($this->faker->name, ENT_QUOTES),
                'updatedAt' => (int) $this->faker->dateTimeBetween('-2 years', '-1 minute')->format('U'),
                //'tags' => implode('|', $this->faker->words($this->faker->numberBetween(3, 15), $asText = false)),
            ];
            if ($primeCount < $primeQty) {
                $person['company'] = htmlentities($this->faker->company, ENT_QUOTES);
                $primeCount ++;
            } else {
                $person['setDate'] = (int) ($chanceOfSetDateNotZero ? $this->faker->dateTimeBetween('-2 years', '-5 minutes')->format('U') : 0);
            }

            $newPerson = new Graph\Node('Person', $person);

            yield $username => $newPerson;
        }
    }

    public function deleteGraph(): Graph
    {
        $graph = new Graph('social', $this->redis);
        try {
            $graph->delete();
        } catch (\RedisException $e) {
            // it's OK if it does not exist.
        }

        return $graph;
    }

    public function showAllPersons(Graph $graph): void
    {
        $matchLink = 'MATCH (p:Person) RETURN p.username, p.name, p.company, p.setDate';
        $result = $graph->query($matchLink);
        //$result->prettyPrint();
    }

    private function generatePersonsAndLinks(int $qty, int $primeQty): array
    {
        $this->stopwatch->start('build-persons');

        $this->persons = [];
        $this->primePersons = [];
        $otherPersons = [];
        foreach ($this->buildPersons($qty, $primeQty) as $username => $person) {
            $this->persons[$username] = $person;
            if (isset($person->properties['company'])) {
                $this->primePersons[$username] = $person->properties['username'];
            } else {
                $otherPersons[$username] = $person->properties['username'];
            }
        }

        $numOtherPersons = count($otherPersons);
        $maxAllowedConnections = min(2500, $numOtherPersons);    // no more than 2500 per 'prime'

        $totalConnections = 0;
        $this->linkPersons = [];
        foreach ($this->primePersons as $username => $person) {
            // Pick a number of usernames [0=>'johndoe', 1=>...]
            $numConnections = random_int(50, $maxAllowedConnections);
            $this->linkPersons[$username] = array_rand($otherPersons, $numConnections);

            $totalConnections += $numConnections;
        }
        
        $this->stopwatch->stop('build-persons');

        return [$totalConnections];
    }

    private function printCountOfLinksPerPerson(Graph $graph): void
    {
        $query = 'MATCH (r:Person)-[x:link]->(:Person) RETURN r.username,COUNT(x)';
        $result = $graph->query($query);

        echo "\nTotal number of links\n";
        $result->prettyPrint();
    }

    private function showAllConnectionsForRandomPrimePerson(Graph $graph): void
    {
        $this->stopwatch->start('search-connections-for-random-prime');

        $username = array_rand($this->primePersons, 1);
        $query = "MATCH (r:Person)-[:link]->(c:Person) WHERE r.username = '{$username}' RETURN r.username,c.username,c.name,c.setDate";
        $result = $graph->query($query);
        $this->stopwatch->stop('search-connections-for-random-prime');

        echo "\nAll links from {$username}\n";
        $result->prettyPrint();
    }

    /**
     * Put the pre-generated persons into the DB, a group at a time
     */
    private function buildAllPersons(OutputInterface $output, Graph $graph): void
    {
        $INSERT_PER_GROUP = 128;
        
        echo "\nBuilding person nodes.\n";
        $progressBar = new ProgressBar($output, count($this->persons));
        $progressBar->start();

        $cnt = 0;
        $this->stopwatch->start('insert-persons');
        // Create all the nodes
        foreach ($this->persons as $username => $person) {
            $graph->addNode($person);

            $cnt++;
            if (($cnt % $INSERT_PER_GROUP) === 0) {
                $graph->commit();   // save this group to DB
                $graph->nodes = $graph->edges = [];
                $progressBar->setProgress($cnt);
            }
        }
        $graph->commit();
        $progressBar->finish();

        $this->redis->rawCommand('GRAPH.QUERY', $graph->name, 'CREATE INDEX ON :Person(username)');
        $this->redis->rawCommand('GRAPH.QUERY', $graph->name, 'CREATE INDEX ON :Person(setDate)');

        $this->stopwatch->stop('insert-persons');

        // empty out the internal records to save memory
        $graph->nodes = $graph->edges = [];
    }

    private function buildLinksBetweenPersons(OutputInterface $output, $totalConnections, Graph $graph): void
    {
        echo "\nBuild connections.\n";
        $progressBar = new ProgressBar($output, $totalConnections);
        $progressBar->start();

        $this->stopwatch->start('build-edges');

        // build edges among the nodes, writes edges as needed  
        $cnt = 0;
        foreach ($this->linkPersons as $username => $linkToPeople) {
            $now = time();
            $linkProps = "{createdAt: $now, src:'manual'}";

            foreach ($linkToPeople as $linkToPerson) {
                $query = "MATCH (r:Person),(c:Person) WHERE r.username = '{$username}' AND c.username = '{$linkToPerson}' CREATE (r)-[:link $linkProps]->(c)";
                $graph->query($query);

                $cnt++;
                if (($cnt % 128) === 0) {
                    $progressBar->setProgress($cnt);
                }
            }
        }

        $this->stopwatch->stop('build-edges');

        $progressBar->setProgress($cnt);
        $progressBar->finish();
        echo "\n{$cnt} connections created.\n";
    }

    private function benchmarkGetLinksWithQuery(Graph $graph)
    {
        // show all the connections for one of the 'Primes', with a date query as well
        $this->stopwatch->start('search-links');

        $username = array_rand($this->primePersons, 1);
        $today = (int) date('U');
        $query = "MATCH (r:Person)-[:link]->(c:Person) WHERE (r.username='{$username}') AND (c.setDate > 0) AND (c.setDate < {$today}) RETURN r.username,c.username,c.name,c.setDate,c.updatedAt  ORDER BY r.username,c.username,c.setDate";
        $result = $graph->query($query);

        $this->stopwatch->stop('search-links');
        //$result->prettyPrint();
        unset($result);
    }

    private function displaySummary(OutputInterface $output, $totalConnections): void
    {
        $buildPersonsDuration = $this->stopwatch->getEvent('build-persons')->getDuration();
        $buildEdgesDuration = $this->stopwatch->getEvent('build-edges')->getDuration();
        $searchLinksDuration = $this->stopwatch->getEvent('search-links')->getDuration();

        $buildPersonsPerSec = round((count($this->persons) / $buildPersonsDuration) * 1000, 2);
        $buildEdgesPerSec = round(($totalConnections / $buildEdgesDuration) * 1000, 2);

        $rightAligned = new TableStyle();
        $rightAligned->setPadType(STR_PAD_LEFT);
        
        $table = new Table($output);
        $table->setColumnStyle(1, $rightAligned);
        $table->setHeaders(array('', 'Counts/Duration'))->setRows(
            [
                ['persons built', count($this->persons)],
                ['total connections', $totalConnections],
                ['build persons per/sec', $buildPersonsPerSec],
                ['buildEdges per/sec', $buildEdgesPerSec],
                ['build persons duration', $buildPersonsDuration.'ms'],
                ['build-edges duration', $buildEdgesDuration.'ms'],
                ['search links duration', $searchLinksDuration.'ms'],
            ]
        );
        $table->render();
    }
}
