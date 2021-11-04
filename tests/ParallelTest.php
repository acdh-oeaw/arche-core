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
        list($resp1, $resp2) = $this->runConcurrently([$req1, $req2], 1000);
        $codes   = [$resp1->getStatusCode(), $resp2->getStatusCode()];
        $this->assertContains(200, $codes);
        $this->assertContains(409, $codes);
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
        list($resResp, $txResp) = $this->runConcurrently([$resReq, $txReq], 1000);
        print_r($resResp->getHeaders());
        print_r($txResp->getHeaders());
        print_r([$resResp->getStatusCode(), $txResp->getStatusCode()]);
        $this->assertEquals(200, $resResp->getStatusCode());
        $this->assertEquals(409, $txResp->getStatusCode());
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
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
        ];
        $req1    = new Request('put', self::$baseUrl . 'transaction', $headers);
        $req2    = new Request('get', self::$baseUrl . 'transaction', $headers);
        $resp    = $this->runConcurrently([$req1, $req2, $req2, $req2, $req2], 10000);
        $this->assertEquals(204, $resp[0]->getStatusCode());
        $i       = 1;
        while ($i < count($resp) && $resp[$i]->getStatusCode() === 200) {
            $i++;
        }
        $this->assertLessThan(count($resp), $i);
        $this->assertEquals(400, $resp[$i]->getStatusCode());
        $lastData = json_decode($resp[$i - 1]->getBody());
        $this->assertEquals(Transaction::STATE_COMMIT, $lastData->state);
    }
    
    //TODO - test assuring some kind of locking prevents long processing from transaction remocal
    // SearchTest::testFullTextSearch2 does the job
}
