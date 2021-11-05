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

use EasyRdf\Graph;
use EasyRdf\Literal;
use EasyRdf\Resource;
use GuzzleHttp\Psr7\Request;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\Metadata;
use acdhOeaw\arche\core\Transaction;

/**
 * Tests 
 *
 * @author zozlak
 */
class ParallelTest extends TestBase {

    static public function sleepResource(int $id, Resource $meta, ?string $path): Resource {
        usleep(100000); // sleep 100 ms
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
            'txCommit'       => [
                'type'     => 'function',
                'function' => self::class . '::sleepTx',
            ],
            'updateMetadata' => [
                'type'     => 'function',
                'function' => self::class . '::sleepResource',
            ],
        ];
        $cfg      = yaml_parse_file(__DIR__ . '/../config.yaml');
        foreach ($handlers as $method => $data) {
            $cfg['rest']['handlers']['methods'][$method][] = $data;
        }
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
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
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 10000);
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
        $resp = $this->runConcurrently($requests, [50000, 25000]);
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
        $this->assertEquals('value1', $r1->getLiteral($prop));
        $this->assertEquals('value2', $r2->getLiteral($prop));
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
    //TODO - test assuring some kind of locking prevents long processing from transaction remocal
    // SearchTest::testFullTextSearch2 does the job
}
