<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\core\tests;

use DateTime;
use PDO;
use RuntimeException;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\NamedNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\NQuadsSerializer;
use quickRdfIo\Util as RdfIoUtil;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;
use acdhOeaw\arche\core\Metadata;
use acdhOeaw\arche\lib\Config;
use acdhOeaw\arche\core\util\Schema;

/**
 * Description of TestBase
 *
 * @author zozlak
 */
class TestBase extends \PHPUnit\Framework\TestCase {

    const BINARY_RES_PATH = __DIR__ . '/data/test.ttl';

    static protected string $baseUrl;
    static protected Client $client;
    static protected Config $config;
    static protected Schema $schema;
    static protected NamedNode $baseNode;
    static protected mixed $txCtrl;
    static protected NQuadsSerializer $serializer;

    /**
     *
     * @var \PDO
     */
    static protected $pdo;

    static public function setUpBeforeClass(): void {
        file_put_contents(__DIR__ . '/../config.yaml', file_get_contents(__DIR__ . '/config.yaml'));

        self::$client     = new Client(['http_errors' => false, 'allow_redirects' => false]);
        self::$config     = Config::fromYaml(__DIR__ . '/../config.yaml');
        self::$schema     = Schema::fromConfig(self::$config);
        self::$baseUrl    = self::$config->rest->urlBase . self::$config->rest->pathBase;
        self::$baseNode   = DF::namedNode(self::$baseUrl);
        self::$serializer = new NQuadsSerializer();
        self::$pdo        = new PDO(self::$config->dbConn->admin);
        self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        self::$pdo->query("SET application_name TO test_conn");

        $cmd          = ['php', '-f', __DIR__ . '/../transactionDaemon.php', __DIR__ . '/../config.yaml'];
        $pipes        = [];
        self::$txCtrl = proc_open($cmd, [], $pipes, __DIR__ . '/../');
        if (self::$txCtrl === false) {
            throw new RuntimeException('failed to start the transactionDaemon');
        }

        // give services like the transaction manager or tika time to start
        usleep(500000);
        self::reloadTxCtrlConfig();
    }

    static public function tearDownAfterClass(): void {
        proc_terminate(self::$txCtrl, 15);
        while (proc_get_status(self::$txCtrl)['running']) {
            usleep(100000);
        }
        proc_close(self::$txCtrl);
    }

    static protected function reloadTxCtrlConfig(): void {
        // proc_open() runs the command by invoking shell, so the actual process's PID is (if everything goes fine) one greater
        $s = proc_get_status(self::$txCtrl);
        posix_kill($s['pid'] + 1, 10);
        usleep(100000);
    }

    /**
     * 
     * @param array<string, string> $handlers
     * @return void
     */
    static protected function setHandler(array $handlers): void {
        $cfg = yaml_parse_file(__DIR__ . '/../config.yaml');
        foreach ($handlers as $method => $function) {
            $cfg['rest']['handlers']['methods'][$method] = [
                [
                    'type'     => 'function',
                    'function' => $function,
                ]
            ];
        }
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
    }

    public function setUp(): void {
        self::$pdo->query("TRUNCATE transactions CASCADE");
    }

    public function tearDown(): void {
        
    }

    protected function beginTransaction(): int {
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        return (int) ($resp->getHeader(self::$config->rest->headers->transactionId)[0] ?? throw new RuntimeException("Failed to begin a transaction"));
    }

    protected function commitTransaction(int $txId): int {
        $req  = new Request('put', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        return $resp->getStatusCode();
    }

    protected function rollbackTransaction(int $txId): int {
        $req  = new Request('delete', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        return $resp->getStatusCode();
    }

    protected function waitForTransactionEnd(int $txId, int $tMax = 5): void {
        $req  = new Request('get', self::$baseUrl . 'transaction?transactionId=' . $txId);
        $resp = null;
        while ($tMax > 0 && $resp?->getStatusCode() !== 400) {
            usleep(500000);
            $resp = self::$client->send($req);
            $tMax -= 0.5;
        }
        $this->assertGreaterThan(0, $tMax, "Timeout while waiting for the transaction end");
    }

    protected function createMetadata(?string $uri = null): DatasetNode {
        $r = DF::namedNode($uri ?? self::$baseUrl);
        $g = new DatasetNode($r);
        $g->add([
            DF::quad($r, DF::namedNode(self::$config->schema->id), DF::namedNode('https://' . rand())),
            DF::quad($r, DF::namedNode('http://test/hasRelation'), DF::namedNode('https://' . rand())),
            DF::quad($r, DF::namedNode('http://test/hasTitle'), DF::literal('title')),
            DF::quad($r, DF::namedNode('http://test/hasDate'), DF::literal((new DateTime())->format(DateTime::ISO8601))),
            DF::quad($r, DF::namedNode('http://test/hasNumber'), DF::literal(123.5)),
        ]);
        return $g;
    }

    protected function createBinaryResource(?int $txId = null): string {
        $extTx = $txId !== null;
        if (!$extTx) {
            $txId = $this->beginTransaction();
        }

        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Disposition'                       => 'attachment; filename="test.ttl"',
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];
        $body    = (string) file_get_contents(self::BINARY_RES_PATH);
        $req     = new Request('post', self::$baseUrl, $headers, $body);
        $resp    = self::$client->send($req);

        if (!$extTx) {
            $this->commitTransaction($txId);
        }
        if ($resp->getStatusCode() >= 400) {
            throw new RuntimeException((string) $resp->getBody(), $resp->getStatusCode());
        }
        return $resp->getHeader('Location')[0];
    }

    protected function createMetadataResource(Dataset | DatasetNode | null $meta = null,
                                              ?int $txId = null): string {
        $meta ??= new Dataset();
        if ($meta instanceof DatasetNode) {
            $meta = $meta->getDataset();
        }
        $meta->forEach(fn($x) => $x->withSubject(DF::namedNode(self::$baseUrl)));

        $extTx = $txId !== null;
        if (!$extTx) {
            $txId = $this->beginTransaction();
        }

        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];
        $body    = self::$serializer->serialize($meta);
        $req     = new Request('post', self::$baseUrl . 'metadata', $headers, $body);
        $resp    = self::$client->send($req);

        if (!$extTx) {
            $this->commitTransaction($txId);
        }
        if ($resp->getStatusCode() >= 400) {
            throw new RuntimeException((string) $resp->getBody(), $resp->getStatusCode());
        }
        return $resp->getHeader('Location')[0];
    }

    protected function updateResource(DatasetNode $meta, ?int $txId = null,
                                      string $mode = Metadata::SAVE_MERGE): ResponseInterface {
        $extTx = $txId !== null;
        if (!$extTx) {
            $txId = $this->beginTransaction();
        }

        $headers = [
            self::$config->rest->headers->transactionId     => $txId,
            self::$config->rest->headers->metadataWriteMode => $mode,
            'Content-Type'                                  => 'application/n-triples',
            'Eppn'                                          => 'admin',
        ];
        $body    = self::$serializer->serialize($meta);
        $req     = new Request('patch', $meta->getNode()->getValue() . '/metadata', $headers, $body);
        $resp    = self::$client->send($req);

        if (!$extTx) {
            $this->commitTransaction($txId);
        }

        return $resp;
    }

    protected function deleteResource(string $location, ?int $txId = null): bool {
        $extTx = $txId !== null;
        if (!$extTx) {
            $txId = $this->beginTransaction();
        }

        $req  = new Request('delete', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);

        if (!$extTx) {
            $this->commitTransaction($txId);
        }

        return $resp->getStatusCode() === 204;
    }

    /**
     * 
     * @param int $txId
     * @return array<string, mixed>
     */
    protected function getHeaders(?int $txId = null): array {
        return [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
        ];
    }

    protected function extractResource(ResponseInterface | StreamInterface | string $body,
                                       ?string $location = null): DatasetNode {
        $graph    = new Dataset();
        $graph->add(RdfIoUtil::parse($body, new DF()));
        $location = $location === null ? $graph->getSubject() : DF::namedNode($location);
        return (new DatasetNode($location))->withDataset($graph);
    }

    protected function getResourceMeta(string $location): DatasetNode {
        $req  = new Request('get', $location . '/metadata');
        $resp = self::$client->send($req);
        return $this->extractResource($resp, $location);
    }

    /**
     * 
     * @param array<mixed> $opts
     * @param string $method
     * @return Dataset
     */
    protected function runSearch(array $opts, string $method = 'get'): Dataset {
        $resp  = self::$client->request($method, self::$baseUrl . 'search', $opts);
        $graph = new Dataset();
        $graph->add(RdfIoUtil::parse($resp, new DF()));
        return $graph;
    }

    /**
     * Runs given requests in parallel, keeping a given delay between sending them.
     * 
     * @param array<Request> $requests
     * @param int|array<int> $delayUs
     * @return array<Response>
     */
    protected function runConcurrently(array $requests, $delayUs = 0): array {
        if (!is_array($delayUs)) {
            $delayUs = [$delayUs];
        }
        $lastDelay = $delayUs[count($delayUs) - 1];
        while (count($delayUs) < count($requests)) {
            $delayUs[] = $lastDelay;
        }

        $handle       = curl_multi_init();
        $reqHandles   = [];
        $outputs      = [];
        $startTimes   = [];
        $runningCount = null;
        foreach ($requests as $n => $i) {
            /* @var $i Request */
            $req          = curl_init();
            $reqHandles[] = $req;
            curl_setopt($req, \CURLOPT_URL, $i->getUri());
            curl_setopt($req, \CURLOPT_CUSTOMREQUEST, $i->getMethod());
            $headers      = [];
            foreach ($i->getHeaders() as $header => $values) {
                $headers[] = $header . ": " . implode(', ', $values);
            }
            curl_setopt($req, \CURLOPT_HTTPHEADER, $headers);
            curl_setopt($req, \CURLOPT_POSTFIELDS, $i->getBody()->getContents());
            $output       = fopen("php://memory", "rw");
            $outputs[]    = $output;
            curl_setopt($req, \CURLOPT_RETURNTRANSFER, true);
            curl_setopt($req, \CURLOPT_FILE, $output);
            curl_multi_add_handle($handle, $req);
            $startTimes[] = microtime(true);
            curl_multi_exec($handle, $runningCount);
            if ($delayUs[$n] > 0) {
                usleep($delayUs[$n]);
            }
        }
        do {
            $status = curl_multi_exec($handle, $runningCount);
            if ($runningCount > 0) {
                curl_multi_select($handle);
            }
        } while ($runningCount > 0 && $status === \CURLM_OK);
        $responses = [];
        foreach ($reqHandles as $n => $i) {
            curl_multi_remove_handle($handle, $i);
            $code        = curl_getinfo($i, \CURLINFO_RESPONSE_CODE);
            fseek($outputs[$n], 0);
            $headers     = [
                'Start-Time'   => $startTimes[$n],
                'Time'         => curl_getinfo($i, \CURLINFO_TOTAL_TIME_T),
                'Content-Type' => curl_getinfo($i, \CURLINFO_CONTENT_TYPE),
            ];
            $responses[] = new Response($code, $headers, $outputs[$n]);
        }
        curl_multi_close($handle);
        return $responses;
    }
}
