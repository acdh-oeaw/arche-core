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
use GuzzleHttp\Psr7\Request;
use function \GuzzleHttp\json_encode;

/**
 * Description of TransactionTest
 *
 * @author zozlak
 */
class TransactionTest extends TestBase {

    /**
     * @group transactions
     */
    public function testGet(): void {
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $txId = ((int) $resp->getHeader(self::$config->rest->headers->transactionId)[0]) ?? null;
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data = json_decode($resp->getBody());
        $this->assertEquals($txId, $data->transactionId);
        $this->assertEquals('active', $data->state);
    }

    /**
     * @group transactions
     */
    public function testProlong(): void {
        $txId = $this->beginTransaction();
        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));

        sleep(self::$config->transactionController->timeout / 2);
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data = json_decode($resp->getBody());
        $this->assertEquals($txId, $data->transactionId);
        $this->assertEquals('active', $data->state);

        sleep(self::$config->transactionController->timeout / 2);
        $resp = self::$client->send($req);
        sleep(self::$config->transactionController->timeout / 2);
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data = json_decode($resp->getBody());
        $this->assertEquals($txId, $data->transactionId);
        $this->assertEquals('active', $data->state);
    }

    /**
     * @group transactions
     */
    public function testExpires(): void {
        $txId = $this->beginTransaction();
        sleep(self::$config->transactionController->timeout * 2);

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());

        $req  = new Request('delete', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());

        $req  = new Request('put', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());
    }

    /**
     * @group transactions
     */
    public function testEmpty(): void {
        // commit
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $txId = ((int) $resp->getHeader(self::$config->rest->headers->transactionId)[0]) ?? null;
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('put', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());

        // rollback
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $txId = ((int) $resp->getHeader(self::$config->rest->headers->transactionId)[0]) ?? null;
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('delete', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
    }

    /**
     * @group transactions
     */
    public function testCreateRollback(): void {
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $location = $this->createBinaryResource($txId);

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__DIR__ . '/data/test.ttl'), $resp->getBody(), 'created file content mismatch');

        $this->assertEquals(204, $this->rollbackTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

    /**
     * @group transactions
     */
    public function testDeleteRollback(): void {
        // create a resource and make sure it's there
        $location = $this->createBinaryResource();
        $req      = new Request('get', $location, $this->getHeaders());
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        // begin a transaction
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        // delete the resource and make sure it's not there
        $req  = new Request('delete', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new Graph();
        $meta->parse($resp->getBody());
        $this->assertEquals($location, (string) $meta->resource($location)->getResource(self::$config->schema->id));

        $req  = new Request('delete', $location . '/tombstone', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());

        // rollback the transaction and check if the resource is back
        $this->assertEquals(204, $this->rollbackTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__DIR__ . '/data/test.ttl'), $resp->getBody(), 'file content mismatch');
    }

    /**
     * @group transactions
     */
    public function testPatchMetadataRollback(): void {
        // set up and remember an initial state
        $location = $this->createBinaryResource();

        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res1 = $this->extractResource($resp, $location);

        // PATCH
        $txId = $this->beginTransaction();

        $meta    = $this->createMetadata($location);
        $headers = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req     = new Request('patch', $location . '/metadata', $headers, $meta->getGraph()->serialise('application/n-triples'));
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res2    = $this->extractResource($resp, $location);
        $this->assertEquals('test.ttl', (string) $res2->getLiteral(self::$config->schema->fileName));
        $this->assertEquals('title', (string) $res2->getLiteral('http://test/hasTitle'));

        $this->rollbackTransaction($txId);

        // make sure nothing changed after transaction commit
        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res3 = $this->extractResource($resp, $location);
        $this->assertEquals('test.ttl', (string) $res3->getLiteral(self::$config->schema->fileName));
        $this->assertEquals(null, $res3->getLiteral('http://test/hasTitle'));
    }

    /**
     * @group transactions
     */
    public function testForeignCheckLoop(): void {
        $txId  = $this->beginTransaction();
        $loc1  = $this->createMetadataResource(null, $txId);
        $meta1 = (new Graph())->resource(self::$baseUrl);
        $meta1->addResource('http://relation', $loc1);
        $loc2  = $this->createMetadataResource($meta1, $txId);
        $meta2 = (new Graph())->resource($loc1);
        $meta2->addResource('http://relation', $loc2);
        $this->updateResource($meta2, $txId);
        $this->rollbackTransaction($txId);

        $req  = new Request('get', $loc1);
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
        $req  = new Request('get', $loc2);
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

    /**
     * @group transactions
     */
    public function testTransactionConflict(): void {
        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);

        $txId1 = $this->beginTransaction();
        $resp  = $this->updateResource($meta, $txId1);
        $this->assertEquals(200, $resp->getStatusCode());

        $txId2 = $this->beginTransaction();
        $resp  = $this->updateResource($meta, $txId2);
        $this->assertEquals(403, $resp->getStatusCode());

        $this->commitTransaction($txId1);
        $resp = $this->updateResource($meta, $txId2);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(204, $this->commitTransaction($txId2));
    }

    /**
     * @group transactions
     */
    public function testPassIdWithinTransaction(): void {
        $meta1 = (new Graph())->resource(self::$baseUrl);
        $meta1->addResource(self::$config->schema->id, 'https://my/id');
        $loc1  = $this->createMetadataResource($meta1);

        $txId = $this->beginTransaction();

        $meta2 = (new Graph())->resource($loc1);
        $meta2->addResource(self::$config->schema->delete, self::$config->schema->id);
        $resp  = $this->updateResource($meta2, $txId);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp, $loc1);
        $this->assertEquals(1, count($meta3->all(self::$config->schema->id)));
        $this->assertEquals($loc1, (string) $meta3->getResource(self::$config->schema->id));

        $loc2  = $this->createMetadataResource($meta1);
        $meta4 = $this->getResourceMeta($loc2);
        $this->assertEquals(2, count($meta4->all(self::$config->schema->id)));
        foreach ($meta4->all(self::$config->schema->id) as $i) {
            $this->assertContains((string) $i, [$loc2, 'https://my/id']);
        }

        $this->assertEquals(204, $this->commitTransaction($txId));
    }

    /**
     * @group transactions
     */
    public function testCompletenessAbort(): void {
        $cfg                                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['transactionController']['enforceCompleteness'] = true;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        self::reloadTxCtrlConfig();

        $txId     = $this->beginTransaction();
        $location = $this->createBinaryResource($txId);
        $req      = new Request('get', $location . '/metadata', $this->getHeaders($txId));
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $req  = new Request('get', $location . '/foo', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());

        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

    /**
     * @group transactions
     */
    public function testOptions(): void {
        $resp = self::$client->send(new Request('options', self::$baseUrl . 'transaction'));
        $this->assertEquals('OPTIONS, POST, HEAD, GET, PUT, DELETE', $resp->getHeader('Allow')[0] ?? '');
    }

    /**
     * @group transactions
     */
    public function testWrongHttpMethod(): void {
        $resp = self::$client->send(new Request('patch', self::$baseUrl . 'transaction'));
        $this->assertEquals(405, $resp->getStatusCode());
    }
}
