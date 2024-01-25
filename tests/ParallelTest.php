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

use GuzzleHttp\Psr7\Request;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\core\Transaction;

/**
 * Tests 
 *
 * @author zozlak
 */
class ParallelTest extends TestBase {

    static public function sleepResource(int $id, DatasetNode $meta,
                                         ?string $path): DatasetNode {
        \acdhOeaw\arche\core\RestController::$log->debug("BEGIN OF SLEEP");
        usleep(100000); // sleep 100 ms
        \acdhOeaw\arche\core\RestController::$log->debug("END OF SLEEP");
        return $meta;
    }

    static public function sleepTx(string $method, int $txId, array $resourceIds): void {
        usleep(100000); // sleep 100 ms
    }

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        // add some latency to request handling to make tests timing more
        // predictable
        $handlers = [
            'txCommit'       => self::class . '::sleepTx',
            'updateMetadata' => self::class . '::sleepResource',
        ];
        self::setHandler($handlers);
    }

    public function testIssue24(): void {
        $loc  = $this->createMetadataResource();
        $conn = pg_connect(substr(self::$config->dbConn->admin, 6));
        pg_query($conn, "SET application_name TO test_side_conn");
        pg_query($conn, "BEGIN");

        $tx      = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $tx,
            'Content-Type'                              => 'application/n-triples',
            'Eppn'                                      => 'admin',
        ];
        // prepare large-enough metadata
        $r       = DF::namedNode($loc);
        $meta    = new DatasetNode($r);
        $meta->add([
            DF::quad($r, DF::namedNode('http://foo/a'), DF::literal(str_repeat("a", 1000))),
            DF::quad($r, DF::namedNode('http://foo/b'), DF::literal(str_repeat("b", 1000))),
            DF::quad($r, DF::namedNode('http://foo/c'), DF::literal(str_repeat("c", 1000))),
            DF::quad($r, DF::namedNode('http://foo/d'), DF::literal(str_repeat("d", 1000))),
            DF::quad($r, DF::namedNode('http://foo/e'), DF::literal(str_repeat("e", 1000))),
            DF::quad($r, DF::namedNode('http://foo/f'), DF::literal(str_repeat("f", 1000))),
            DF::quad($r, DF::namedNode('http://foo/g'), DF::literal(str_repeat("g", 1000))),
            DF::quad($r, DF::namedNode('http://foo/h'), DF::literal(str_repeat("h", 1000))),
            DF::quad($r, DF::namedNode('http://foo/i'), DF::literal(str_repeat("i", 1000))),
            DF::quad($r, DF::namedNode('http://foo/j'), DF::literal(str_repeat("j", 1000))),
        ]);
        $body    = self::$serializer->serialize($meta);
        $req     = new Request('patch', "$loc/metadata", $headers, $body);

        // run a query locking the transaction during handler execution
        pg_send_query($conn, "SELECT pg_sleep(0.1); UPDATE transactions SET last_request = now() WHERE transaction_id = $tx;");
        $resp = self::$client->send($req);
        // release database locks so everything can safely end
        pg_cancel_query($conn);
        pg_query($conn, "ROLLBACK;");
        pg_query($conn, "UPDATE resources SET transaction_id = null;");
        pg_close($conn);
        $this->rollbackTransaction($tx);

        // check if 409 has been captured
        $this->assertEquals(409, $resp->getStatusCode());
        $this->assertStringContainsString('canceling statement due to lock timeout', (string) $resp->getBody());
    }

    /**
     * tx commit + tx rollback
     * - The tx commit should succeed
     * - The tx rollback should fail with HTTP 409
     * 
     * @group parallel
     */
    public function testParallelCommitRollback(): void {
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
        ];
        $req1    = new Request('put', self::$baseUrl . 'transaction', $headers);
        $req2    = new Request('delete', self::$baseUrl . 'transaction', $headers);
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 50000);
        $this->assertEquals(204, $resp1->getStatusCode());
        $this->assertEquals(409, $resp2->getStatusCode());
    }

    /**
     * tx commit + tx get
     * - The tx commit should succeed
     * - The tx get should succeed
     * 
     * @group parallel
     */
    public function testParallelCommitTxGet(): void {
        $txId     = $this->beginTransaction();
        $headers  = [
            self::$config->rest->headers->transactionId => $txId,
        ];
        $requests = [new Request('put', self::$baseUrl . 'transaction', $headers)];
        while (count($requests) < 10) {
            $requests[] = new Request('get', self::$baseUrl . 'transaction', $headers);
        }
        $resp = $this->runConcurrently($requests, 75000);
        $this->assertEquals(204, $resp[0]->getStatusCode());
        $i    = 1;
        while ($i < count($resp) && $resp[$i]->getStatusCode() === 200) {
            $i++;
        }
        $this->assertLessThan(count($resp), $i);
        $this->assertEquals(400, $resp[$i]->getStatusCode());
        $lastData = json_decode($resp[$i - 1]->getBody());
        // We can't check just for the STATE_COMMIT because the TransactionController
        // runs asynchronously and it's impossible to predict the time between
        // the release of the transaction done by the `PUT /transaction` and
        // the transaction removal perfromed by the TransactionController.
        // Sending many `GET /transaction` requests in a short period of time
        // doesn't help as it makes all the timings even less predictable.
        $this->assertContains($lastData->state, [Transaction::STATE_COMMIT, Transaction::STATE_ACTIVE]);
    }

    /**
     * patch + tx commit
     * - The patch should pass
     * - The tx commit should fail with 409 because of the patch
     * 
     * @group parallel
     */
    public function testParallelPatchAndCommit(): void {
        $location = $this->createMetadataResource();
        $prop     = 'http://foo';

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $resReq  = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value1\" .");
        $txReq   = new Request('put', self::$baseUrl . 'transaction', $headers);
        list($resResp, $txResp) = $this->runConcurrently([$resReq, $txReq], 50000);
        $this->assertEquals(200, $resResp->getStatusCode());
        $this->assertEquals(409, $txResp->getStatusCode());
    }

    /**
     * tx commit + patch
     * - The tx commit should pass
     * - The patch should fail with 409 because of the commit
     * 
     * @group parallel
     */
    public function testParallelCommitAndPatch(): void {
        $location = $this->createMetadataResource();
        $prop     = 'http://foo';

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $txReq   = new Request('put', self::$baseUrl . 'transaction', $headers);
        $resReq  = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value1\" .");
        list($txResp, $resResp) = $this->runConcurrently([$txReq, $resReq], 50000);
        $this->assertEquals(204, $txResp->getStatusCode());
        $this->assertEquals(409, $resResp->getStatusCode());
    }

    /**
     * patch + patch on separate resources
     * Both should pass
     * 
     * @group parallel
     */
    public function testParallelPatchPatchOther(): void {
        $loc1 = $this->createMetadataResource();
        $loc2 = $this->createMetadataResource();
        $prop = 'http://foo';
        $tmpl = new PT($prop);

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $req1    = new Request('patch', $loc1 . '/metadata', $headers, "<$loc1> <$prop> \"value1\" .");
        $req2    = new Request('patch', $loc2 . '/metadata', $headers, "<$loc2> <$prop> \"value2\" .");
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 0);
        $h1      = $resp1->getHeaders();
        $h2      = $resp2->getHeaders();
        $this->assertEquals(200, $resp1->getStatusCode());
        $this->assertEquals(200, $resp2->getStatusCode());
        $this->assertLessThan(min($h1['Time'][0], $h2['Time'][0]) / 2, abs($h1['Start-Time'][0] - $h2['Start-Time'][0])); // make sure they were executed in parallel
        $r1      = $this->extractResource($resp1->getBody(), $loc1);
        $r2      = $this->extractResource($resp2->getBody(), $loc2);
        $this->assertEquals('value1', $r1->getObject($tmpl)?->getValue());
        $this->assertEquals('value2', $r2->getObject($tmpl)?->getValue());
    }

    /**
     * patch + patch to the same resource
     * The first patch should succeed, the second one should fail with 409
     * 
     * @group parallel
     */
    public function testParallelPatchPatchSame(): void {
        $location = $this->createMetadataResource();
        $prop     = 'http://foo';

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $req1    = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value1\" .");
        $req2    = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value2\" .");
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 50000);
        $codes   = [$resp1->getStatusCode(), $resp2->getStatusCode()];
        $this->assertContains(200, $codes);
        $this->assertContains(409, $codes);
    }

    /**
     * patch + patch on separate resources triggering adding of the same third
     * resource
     * Both should pass
     * 
     * @group parallel
     */
    public function testParallelPatchPatchAddId(): void {
        $loc1  = $this->createMetadataResource();
        $loc2  = $this->createMetadataResource();
        $prop  = 'http://foo';
        $value = 'http://same/object';
        $tmpl  = new PT($prop);

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $req1    = new Request('patch', $loc1 . '/metadata', $headers, "<$loc1> <$prop> <$value> .");
        $req2    = new Request('patch', $loc2 . '/metadata', $headers, "<$loc2> <$prop> <$value> .");
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 0);
        $h1      = $resp1->getHeaders();
        $h2      = $resp2->getHeaders();
        $this->assertEquals(200, $resp1->getStatusCode());
        $this->assertEquals(200, $resp2->getStatusCode());
        $this->assertLessThan(min($h1['Time'][0], $h2['Time'][0]) / 2, abs($h1['Start-Time'][0] - $h2['Start-Time'][0])); // make sure they were executed in parallel
        $r1      = $this->extractResource($resp1->getBody(), $loc1);
        $r2      = $this->extractResource($resp2->getBody(), $loc2);
        $this->assertTrue($r1->getObject($tmpl)?->equals($r2->getObject($tmpl)));
    }

    /**
     * post + post with one resource pointing the other and vice versa
     * Both should pass (if lockTimeout is long enought)
     * or one should throw 409 (if lockTimeout is too short)
     * 
     * @group parallel
     */
    public function testParallelPostPostCycle(): void {
        $tmpl    = new PT(self::$schema->parent);
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId    => $txId,
            'Eppn'                                         => 'admin',
            'Content-Type'                                 => 'application/n-triples',
            'Accept'                                       => 'application/n-triples',
            self::$config->rest->headers->metadataReadMode => 'resource',
        ];
        $meta1   = new DatasetNode(self::$baseNode);
        $meta1->add([
            DF::quad(self::$baseNode, self::$schema->label, DF::literal('res1')),
            DF::quad(self::$baseNode, self::$schema->id, DF::namedNode('http://res1')),
            DF::quad(self::$baseNode, self::$schema->parent, DF::namedNode('http://res2')),
        ]);
        $meta2   = new DatasetNode(self::$baseNode);
        $meta2->add([
            DF::quad(self::$baseNode, self::$schema->label, DF::literal('res2')),
            DF::quad(self::$baseNode, self::$schema->id, DF::namedNode('http://res2')),
            DF::quad(self::$baseNode, self::$schema->parent, DF::namedNode('http://res1')),
        ]);

        $req1  = new Request('post', self::$baseUrl . "metadata", $headers, self::$serializer->serialize($meta1));
        $req2  = new Request('post', self::$baseUrl . "metadata", $headers, self::$serializer->serialize($meta2));
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 0);
        $h1    = $resp1->getHeaders();
        $h2    = $resp2->getHeaders();
        $this->assertEquals(201, $resp1->getStatusCode());
        $this->assertEquals(201, $resp2->getStatusCode());
        $this->assertLessThan(min($h1['Time'][0], $h2['Time'][0]) / 2, abs($h1['Start-Time'][0] - $h2['Start-Time'][0])); // make sure they were executed in parallel
        $meta1 = $this->extractResource($resp1);
        $meta2 = $this->extractResource($resp2);
        $this->assertTrue($meta1->getObject($tmpl)?->equals($meta2->getNode()));
        $this->assertTrue($meta2->getObject($tmpl)?->equals($meta1->getNode()));
    }

    /**
     * post + post with same id
     * One should pass, second one should throw 400
     * 
     * @group parallel
     */
    public function testParallelPostPostSameId(): void {
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $meta    = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quad(self::$baseNode, self::$schema->label, DF::literal('res1')),
            DF::quad(self::$baseNode, self::$schema->id, DF::namedNode('http://res1')),
        ]);
        $body    = self::$serializer->serialize($meta);

        $req1     = new Request('post', self::$baseUrl . "metadata", $headers, $body);
        $req2     = new Request('post', self::$baseUrl . "metadata", $headers, $body);
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 0);
        $statuses = [$resp1->getStatusCode(), $resp2->getStatusCode()];
        $this->assertContains(201, $statuses);
        $this->assertContains(409, $statuses);
        $body     = $resp1->getBody() . "\n" . $resp2->getBody();
        $this->assertStringContainsString('duplicate key value violates unique constraint "identifiers_pkey"', $body);
    }

    /**
     * transactions in atomic mode
     * resource create + wrong resource (causing transaction rollback) + resource create (repeated)
     * Last sequence of resource creation requests may succeed for some time,
     * then it may throw 409 (once the transaction is locked),
     * and finally they should start throwing 400 - no such transaction. At that
     * point the first resource should not exist any more.
     * 
     * @group parallel
     */
    public function testParallelAtomicTransaction(): void {
        $cfg                                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['transactionController']['enforceCompleteness'] = true;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $tx      = $this->beginTransaction();
        $loc1    = $this->createMetadataResource(null, $tx);
        $req1    = new Request('get', "$loc1/metadata");
        $headers = [
            self::$config->rest->headers->transactionId => $tx,
            'Content-Type'                              => 'application/n-triples',
            'Eppn'                                      => 'admin',
        ];
        $resp1   = self::$client->send($req1);
        $this->assertEquals(200, $resp1->getStatusCode());

        $meta2 = new DatasetNode(self::$baseNode);
        $meta2->add(DF::quad(self::$baseNode, self::$schema->id, DF::namedNode($loc1)));
        $body2 = self::$serializer->serialize($meta2);
        $req2  = new Request('post', self::$baseUrl . 'metadata', $headers, $body2);

        $req3 = new Request('post', self::$baseUrl . 'metadata', $headers, '');

        $requests  = [$req2, $req3, $req3, $req3, $req3, $req3, $req3, $req3, $req3,
            $req3];
        $delays    = [100, 10000, 10000, 50000, 100000, 100000, 100000, 200000, 200000];
        $responses = $this->runConcurrently($requests, $delays);

        $resp1 = self::$client->send($req1);
        $this->assertEquals(404, $resp1->getStatusCode());

        $this->assertEquals(409, $responses[0]->getStatusCode());
        $allowed    = [201, 409, 400];
        $allowed400 = [
            "Transaction $tx doesn't exist",
            "Wrong transaction state: rollback",
        ];
        for ($i = 1; $i < count($responses); $i++) {
            $sc = $responses[$i]->getStatusCode();
            if ($sc !== 201) {
                $allowed = [409, 400];
                if ($sc !== 409) {
                    $allowed = [400];
                }
            }
            $this->assertContains($sc, $allowed);
            if ($sc === 409) {
                $this->assertEquals("Transaction $tx is in rollback state and can't be locked", (string) $responses[$i]->getBody());
            } else if ($sc === 400) {
                $this->assertContains((string) $responses[$i]->getBody(), $allowed400);
            }
        }
        $this->assertEquals("Transaction $tx doesn't exist", (string) $responses[$i - 1]->getBody());
        sleep(1);
    }
}
