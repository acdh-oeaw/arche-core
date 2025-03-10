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
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\Util as RdfIoUtil;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\BinaryPayload;

/**
 * Description of TransactionTest
 *
 * @author zozlak
 */
class TransactionTest extends TestBase {

    static public function sleepResource(int $id, DatasetNode $meta,
                                         ?string $path): DatasetNode {
        $c = RC::$config->transactionController;
        usleep(($c->timeout << 20) + ($c->checkInterval << 10));
        return $meta;
    }

    /**
     * 
     * @param string $method
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     */
    static public function sleepTx(string $method, int $txId, array $resourceIds): void {
        $c = RC::$config->transactionController;
        usleep(($c->timeout << 20) + ($c->checkInterval << 10));
    }

    public function testGet(): void {
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $txId = (int) ($resp->getHeader(self::$config->rest->headers->transactionId)[0] ?? -1);
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data = json_decode($resp->getBody());
        $this->assertEquals($txId, $data->transactionId);
        $this->assertEquals('active', $data->state);
    }

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

    public function testEmpty(): void {
        // commit
        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $txId = (int) ($resp->getHeader(self::$config->rest->headers->transactionId)[0] ?? -1);
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
        $txId = (int) ($resp->getHeader(self::$config->rest->headers->transactionId)[0] ?? -1);
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('delete', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
    }

    public function testCreateRollback(): void {
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $location = $this->createBinaryResource($txId);
        $globPath = $this->getGlobPath($location);
        $this->assertCount(1, glob($globPath));

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__DIR__ . '/data/test.ttl'), $resp->getBody(), 'created file content mismatch');

        $this->assertEquals(204, $this->rollbackTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
        $this->assertCount(0, glob($globPath . "*"));
    }

    public function testDeleteRollback(): void {
        // create a resource and make sure it's there
        $location = $this->createBinaryResource();
        $globPath = $this->getGlobPath($location);
        $req      = new Request('get', $location, $this->getHeaders());
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertCount(1, glob($globPath));
        $this->assertCount(1, glob($globPath . "*"));

        // begin a transaction
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        // delete the resource and make sure it's not there
        $req  = new Request('delete', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new Dataset();
        $meta->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertTrue($meta->any(new QT(DF::namedNode($location), self::$schema->id, DF::namedNode($location))));
        $this->assertCount(0, glob($globPath));
        $this->assertCount(1, glob($globPath . "." . $txId));

        $req  = new Request('delete', $location . '/tombstone', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $this->assertCount(0, glob($globPath));
        $this->assertCount(1, glob($globPath . "." . $txId));

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());

        // rollback the transaction and check if the resource is back
        $this->assertEquals(204, $this->rollbackTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__DIR__ . '/data/test.ttl'), $resp->getBody(), 'file content mismatch');
        $this->assertCount(1, glob($globPath));
        $this->assertCount(1, glob($globPath . "*"));
    }

    public function testPatchMetadataRollback(): void {
        // set up and remember an initial state
        $location = $this->createBinaryResource();

        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->extractResource($resp, $location);

        // PATCH
        $txId = $this->beginTransaction();

        $meta    = $this->createMetadata($location);
        $headers = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req     = new Request('patch', $location . '/metadata', $headers, self::$serializer->serialize($meta));
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res2    = $this->extractResource($resp, $location);
        $this->assertEquals('test.ttl', $res2->getObjectValue(new PT(self::$schema->fileName)));
        $this->assertEquals('title', $res2->getObjectValue(new PT('http://test/hasTitle')));

        $this->rollbackTransaction($txId);

        // make sure nothing changed after transaction commit
        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res3 = $this->extractResource($resp, $location);
        $this->assertEquals('test.ttl', $res3->getObjectValue(new PT(self::$schema->fileName)));
        $this->assertNull($res3->getObjectValue(new PT('http://test/hasTitle')));
    }

    public function testForeignCheckLoop(): void {
        $relProp = DF::namedNode('http://relation');
        $txId    = $this->beginTransaction();
        $loc1    = $this->createMetadataResource(null, $txId);
        $meta1   = new DatasetNode(self::$baseNode);
        $meta1->add(DF::quad(self::$baseNode, $relProp, DF::namedNode($loc1)));
        $loc2    = $this->createMetadataResource($meta1, $txId);
        $meta2   = new DatasetNode(self::$baseNode);
        $meta2->add(DF::quad(self::$baseNode, $relProp, DF::namedNode($loc2)));
        $this->updateResource($meta2, $txId);
        $this->rollbackTransaction($txId);

        $req  = new Request('get', $loc1);
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
        $req  = new Request('get', $loc2);
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

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

    public function testPassIdWithinTransaction(): void {
        $meta1 = new DatasetNode(self::$baseNode);
        $meta1->add(DF::quad(self::$baseNode, self::$schema->id, DF::namedNode('https://my/id')));
        $loc1  = $this->createMetadataResource($meta1);

        $txId = $this->beginTransaction();

        $meta2 = new DatasetNode(DF::namedNode($loc1));
        $meta2->add(DF::quad(DF::namedNode($loc1), self::$schema->delete, self::$schema->id));
        $resp  = $this->updateResource($meta2, $txId);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp, $loc1);
        $ids   = $meta3->copy(new PT(self::$schema->id));
        $this->assertCount(1, $ids);
        $this->assertEquals($loc1, $ids->getObjectValue());

        $loc2  = $this->createMetadataResource($meta1);
        $meta4 = $this->getResourceMeta($loc2);
        $ids   = $meta4->copy(new PT(self::$schema->id));
        $this->assertCount(2, $ids);
        foreach ($ids->listObjects()->getValues() as $i) {
            $this->assertContains($i, [$loc2, 'https://my/id']);
        }

        $this->assertEquals(204, $this->commitTransaction($txId));
    }

    public function testCompletenessAbort(): void {
        $cfg                                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['transactionController']['enforceCompleteness'] = true;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

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
     * - tx1 creates res1 and res2
     * - tx2 creates res3 pointing to res1
     * - tx1 is rolled back
     * - tx2 is commited
     * res 1 should stay because it's referenced by res3 while res2 should be 
     * deleted by the transaction controller
     */
    public function testParallelCommonResourceRollbackCommit(): void {
        $tx1   = $this->beginTransaction();
        $tx2   = $this->beginTransaction();
        $loc1  = $this->createMetadataResource(null, $tx1);
        $loc2  = $this->createMetadataResource(null, $tx1);
        $meta3 = new DatasetNode(self::$baseNode);
        $meta3->add(DF::quad(self::$baseNode, self::$schema->parent, DF::namedNode($loc1)));
        $loc3  = $this->createMetadataResource($meta3, $tx2);
        $this->rollbackTransaction($tx1);
        $this->commitTransaction($tx2);
        sleep(2);

        $meta3 = $this->getResourceMeta($loc3);
        $this->assertEquals($loc1, $meta3->getObjectValue(new PT(self::$schema->parent)));

        $req1  = new Request('get', "$loc1/metadata");
        $resp1 = self::$client->send($req1);
        $this->assertEquals(200, $resp1->getStatusCode());
        $req2  = new Request('get', "$loc2/metadata");
        $resp2 = self::$client->send($req2);
        $this->assertEquals(404, $resp2->getStatusCode());
    }

    /**
     * - create a metadata-only resource in a separate transaction
     * - upload its binary in the other transaction and rollback this transaction
     */
    public function testMetaToBinaryRollback(): void {
        $loc        = $this->createMetadataResource();
        $rid        = (int) preg_replace('`^.*/`', '', $loc);
        $storageLoc = BinaryPayload::getStorageDir($rid, self::$config->storage->dir, 0, self::$config->storage->levels) . "/$rid";
        $testReq    = new Request('get', $loc, ['Eppn' => 'admin']);

        $tx   = $this->beginTransaction();
        $resp = self::$client->send($testReq);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertEquals("$loc/metadata", $resp->getHeader('Location')[0] ?? '');

        $headers = [
            self::$config->rest->headers->transactionId => $tx,
            'Content-Disposition'                       => 'attachment; filename="test.ttl"',
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];
        $req     = new Request('put', $loc, $headers, (string) file_get_contents(__DIR__ . '/data/test.ttl'));
        $resp    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $resp    = self::$client->send($testReq);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals((string) file_get_contents(__DIR__ . '/data/test.ttl'), (string) $resp->getBody());
        $this->assertFileExists($storageLoc);

        $this->rollbackTransaction($tx);
        $resp = self::$client->send($testReq);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertFileDoesNotExist($storageLoc);
    }

    public function testUpdateBinaryRollback(): void {
        $loc         = $this->createBinaryResource();
        $testReq     = new Request('get', $loc, ['Eppn' => 'admin']);
        $prevContent = (string) self::$client->send($testReq)->getBody();

        $tx      = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $tx,
            'Eppn'                                      => 'admin',
        ];
        $req     = new Request('put', $loc, $headers, "adjusted content");
        $resp    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $resp    = self::$client->send($testReq);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals("adjusted content", (string) $resp->getBody());

        $this->rollbackTransaction($tx);
        $resp = self::$client->send($testReq);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals($prevContent, (string) $resp->getBody());
    }

    public function testOptions(): void {
        $resp = self::$client->send(new Request('options', self::$baseUrl . 'transaction'));
        $this->assertEquals('OPTIONS, POST, HEAD, GET, PUT, DELETE', $resp->getHeader('Allow')[0] ?? '');
    }

    public function testWrongHttpMethod(): void {
        $resp = self::$client->send(new Request('patch', self::$baseUrl . 'transaction'));
        $this->assertEquals(405, $resp->getStatusCode());
    }

    /**
     * Tests scenarios when request processing takes more then TransactionController
     * timeout. If the transaction is rolled back by the TransactionController,
     * the transaction/resource locking system doesn't work well.
     */
    public function testLongProcessing(): void {
        TestBase::setHandler([
            'txCommit'       => self::class . '::sleepTx',
            'updateMetadata' => self::class . '::sleepResource',
        ]);

        $location = $this->createMetadataResource();
        $prop     = 'http://foo';

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];

        $req  = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value1\" .");
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res  = $this->extractResource($resp->getBody(), $location);
        $this->assertEquals('value1', $res->getObjectValue(new PT($prop)));

        $req  = new Request('put', self::$baseUrl . 'transaction', $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
    }
}
