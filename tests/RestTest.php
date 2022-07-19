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

use RuntimeException;
use EasyRdf\Graph;
use EasyRdf\Literal;
use EasyRdf\Resource;
use GuzzleHttp\Psr7\Request;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\Metadata;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;

/**
 * Description of RestTest
 *
 * @author zozlak
 */
class RestTest extends TestBase {

    /**
     * @group rest
     */
    public function testResourceCreate(): void {
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $headers  = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Disposition'                       => 'attachment; filename="test.ttl"',
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];
        $body     = (string) file_get_contents(__DIR__ . '/data/test.ttl');
        $req      = new Request('post', self::$baseUrl, $headers, $body);
        $resp     = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0] ?? '';
        $this->assertNotEmpty($location);
        $metaN1   = (new Graph())->parse((string) $resp->getBody());

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals($body, $resp->getBody(), 'created file content mismatch');

        $req    = new Request('get', $location . '/metadata', $this->getHeaders($txId));
        $resp   = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $graph  = new Graph();
        $metaN2 = $graph->parse($resp->getBody());
        $res    = $graph->resource($location);
        $this->assertEquals('md5:' . md5_file(__DIR__ . '/data/test.ttl'), (string) $res->getLiteral(self::$config->schema->hash));
        $this->assertEquals($metaN1, $metaN2);

        $this->assertEquals(204, $this->commitTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals($body, $resp->getBody(), 'created file content mismatch');
    }

    public function testVariousMetadataFormats(): void {
        $txId = $this->beginTransaction();

        $location = $this->createMetadataResource($this->createMetadata(), $txId);
        $this->assertIsString($location);

        $prevMeta = null;
        $formats  = ['application/n-triples', 'text/turtle', 'application/ld+json',
            'application/rdf+xml'];
        foreach ($formats as $f) {
            $headers['Accept'] = $f;
            $req               = new Request('get', $location . '/metadata', $headers);
            $resp              = self::$client->send($req);
            $this->assertEquals(200, $resp->getStatusCode());
            $this->assertEquals($f, preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0]));
            $meta              = new Graph();
            $body              = (string) $resp->getBody();
            $meta->parse($body, $f);
            if ($prevMeta !== null) {
                $this->assertEquals($prevMeta->countTriples(), $meta->countTriples());
            } else {
                $prevMeta = $meta;
            }
        }

        $req  = new Request('get', $location . '/metadata?format=text/html');
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('text/html', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0]));

        $req  = new Request('get', $location . '/metadata?format=foo/bar');
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Unsupported metadata format requested', $resp->getBody());

        $this->assertEquals(204, $this->rollbackTransaction($txId));
    }

    /**
     * @group rest
     */
    public function testResourceDelete(): void {
        $location = $this->createBinaryResource();

        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('delete', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new Graph();
        $meta->parse($resp->getBody());
        $this->assertEquals($location, (string) $meta->resource($location)->getResource(self::$config->schema->id));

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());

        $this->assertEquals(204, $this->commitTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());
    }

    /**
     * @group rest
     */
    public function testTombstoneDelete(): void {
        $location = $this->createBinaryResource();
        $this->deleteResource($location);

        // make sure tombstone is there
        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());

        // delete tombstone
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('delete', $location . '/tombstone', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());

        $this->assertEquals(204, $this->commitTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

    /**
     * @group rest
     */
    public function testTombstoneDeleteActive(): void {
        $location = $this->createBinaryResource();

        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);
        $req  = new Request('delete', $location . '/tombstone', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(405, $resp->getStatusCode());

        $this->rollbackTransaction($txId);
    }

    /**
     * 
     * @group rest
     */
    public function testDeleteRecursively(): void {
        $relProp = 'http://relation';
        $idProp  = self::$config->schema->id;
        $txId    = $this->beginTransaction();

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addResource($relProp, $loc1);
        $loc2 = $this->createMetadataResource($meta, $txId);

        $headers                                                       = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->metadataParentProperty] = $relProp;

        $req     = new Request('delete', $loc1, $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta    = new Graph();
        $meta->parse($resp->getBody());
        $deleted = [];
        foreach ($meta->resourcesMatching($idProp) as $delres) {
            $resp      = self::$client->send(new Request('get', $delres->getUri()));
            $this->assertEquals(410, $resp->getStatusCode());
            $deleted[] = $delres->getUri();
        }
        $this->assertContains($loc1, $deleted);
        $this->assertContains($loc2, $deleted);

        $this->commitTransaction($txId);

        foreach ($deleted as $delres) {
            $resp = self::$client->send(new Request('get', $delres));
            $this->assertEquals(410, $resp->getStatusCode());
        }
    }

    /**
     * @group rest
     */
    public function testDeleteWithReferences(): void {
        $txId    = $this->beginTransaction();
        $headers = $this->getHeaders($txId);

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addResource('http://relation', $loc1);
        $loc2 = $this->createMetadataResource($meta, $txId);

        $req  = new Request('delete', $loc1, $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(409, $resp->getStatusCode());

        $headers[self::$config->rest->headers->withReferences] = 1;
        $req                                                   = new Request('delete', $loc1, $headers);
        $resp                                                  = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(204, $this->commitTransaction($txId));

        $req  = new Request('get', $loc1);
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());

        $req  = new Request('get', $loc2 . '/metadata');
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new Graph();
        $meta->parse($resp->getBody());
        $this->assertNull($meta->resource($loc1)->getResource('http://relation'));
    }

    /**
     * @group rest
     */
    public function testForeignCheckSeparateTx(): void {
        $txId = $this->beginTransaction();
        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addResource('http://relation', $loc1);
        $this->createMetadataResource($meta, $txId);
        $this->commitTransaction($txId);

        $txId = $this->beginTransaction();
        $req  = new Request('delete', $loc1, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(409, $resp->getStatusCode());
    }

    /**
     * @group rest
     */
    public function testForeignCheckSameTx(): void {
        $txId = $this->beginTransaction();

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addResource('http://relation', $loc1);
        $this->createMetadataResource($meta, $txId);

        $req  = new Request('delete', $loc1, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(409, $resp->getStatusCode());
    }

    /**
     * @group rest
     */
    public function testHead(): void {
        $location = $this->createBinaryResource();

        $req  = new Request('head', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('attachment; filename="test.ttl"', $resp->getHeader('Content-Disposition')[0] ?? '');
        $this->assertEquals('text/turtle;charset=UTF-8', $resp->getHeader('Content-Type')[0] ?? '');
        $this->assertEquals(541, $resp->getHeader('Content-Length')[0] ?? '');

        $headers = array_merge($this->getHeaders(), ['Accept' => 'application/n-triples']);
        $req     = new Request('head', $location . '/metadata', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('application/n-triples', $resp->getHeader('Content-Type')[0] ?? '');

        $headers = array_merge($this->getHeaders(), ['Accept' => 'text/*']);
        $req     = new Request('head', $location . '/metadata', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('text/turtle;charset=UTF-8', $resp->getHeader('Content-Type')[0] ?? '');
    }

    /**
     * @group rest
     */
    public function testOptions(): void {
        $resp = self::$client->send(new Request('options', self::$baseUrl));
        $this->assertEquals('OPTIONS, POST', $resp->getHeader('Allow')[0] ?? '');

        $resp = self::$client->send(new Request('options', self::$baseUrl . 'metadata'));
        $this->assertEquals('OPTIONS, POST', $resp->getHeader('Allow')[0] ?? '');

        $resp = self::$client->send(new Request('options', self::$baseUrl . '1'));
        $this->assertEquals('OPTIONS, HEAD, GET, PUT, DELETE', $resp->getHeader('Allow')[0] ?? '');

        $resp = self::$client->send(new Request('options', self::$baseUrl . '1/metadata'));
        $this->assertEquals('OPTIONS, HEAD, GET, PATCH', $resp->getHeader('Allow')[0] ?? '');

        $resp = self::$client->send(new Request('options', self::$baseUrl . '1/tombstone'));
        $this->assertEquals('OPTIONS, DELETE', $resp->getHeader('Allow')[0] ?? '');
    }

    /**
     * @group rest
     */
    public function testPut(): void {
        // create a resource and make sure it's there
        $location = $this->createBinaryResource();
        $req      = new Request('get', $location, $this->getHeaders());
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId    => $txId,
            self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            'Content-Disposition'                          => 'attachment; filename="RestTest.php"',
            'Content-Type'                                 => 'application/php',
            'Eppn'                                         => 'admin',
        ];
        $body    = (string) file_get_contents(__FILE__);
        $req     = new Request('put', $location, $headers, $body);
        $resp    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $this->assertEmpty((string) $resp->getBody());

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__FILE__), $resp->getBody(), 'file content mismatch');
        $this->assertStringContainsString("<$location/metadata>; rel=\"alternate\"", $resp->getHeader('Link')[0] ?? '');

        $this->commitTransaction($txId);

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(file_get_contents(__FILE__), $resp->getBody(), 'file content mismatch');
    }

    /**
     * @group rest
     */
    public function testPutReturnMeta(): void {
        // create a resource and make sure it's there
        $location = $this->createBinaryResource();
        $req      = new Request('get', $location, $this->getHeaders());
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId    => $txId,
            self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            'Content-Disposition'                          => 'attachment; filename="RestTest.php"',
            'Content-Type'                                 => 'application/php',
            'Eppn'                                         => 'admin',
        ];
        $body    = (string) file_get_contents(__FILE__);
        $req     = new Request('put', $location, $headers, $body);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $meta = $this->extractResource($resp, $location);
        $this->assertEquals('RestTest.php', (string) $meta->getLiteral(self::$config->schema->fileName));
        $this->assertEquals('application/php', (string) $meta->getLiteral(self::$config->schema->mime));

        $this->rollbackTransaction($txId);
    }

    /**
     * @group rest
     */
    public function testResourceCreateMetadata(): void {
        $idProp = self::$config->schema->id;

        $txId = $this->beginTransaction();

        $meta    = $this->createMetadata();
        $headers = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req     = new Request('post', self::$baseUrl . 'metadata', $headers, $meta->getGraph()->serialise('application/n-triples'));
        $resp    = self::$client->send($req);

        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0];
        $metaN1   = (new Graph())->parse((string) $resp->getBody());

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertEquals($location . '/metadata', $resp->getHeader('Location')[0]);

        $req     = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $graph   = new Graph();
        $body    = (string) $resp->getBody();
        $metaN2  = $graph->parse($body, preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0]));
        $this->assertEquals($metaN1, $metaN2);
        $res     = $graph->resource($location);
        $this->assertEquals(2, count($res->allResources($idProp)));
        $allowed = [$location, (string) $meta->getResource($idProp)];
        foreach ($res->allResources($idProp) as $i) {
            $this->assertTrue(in_array((string) $i, $allowed));
        }
        $this->assertMatchesRegularExpression('|^' . self::$baseUrl . '[0-9]+$|', (string) $res->getResource('http://test/hasRelation'));
        $this->assertEquals('title', (string) $res->getLiteral('http://test/hasTitle'));
        $this->assertEquals(date('Y-m-d'), substr((string) $res->getLiteral('http://test/hasDate'), 0, 10));
        $this->assertEquals(123.5, (string) $res->getLiteral('http://test/hasNumber'));

        $this->commitTransaction($txId);

        // check if everything is still in place after the transaction end
        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals((string) $body, (string) $resp->getBody());
    }

    /**
     * @group rest
     */
    public function testPatchMetadataMerge(): void {
        $location = $this->createBinaryResource();
        $meta1    = $this->getResourceMeta($location);

        $g     = new Graph();
        $meta2 = $g->resource($location);
        $meta2->addResource(self::$config->schema->id, 'https://123');
        $meta2->addLiteral('http://test/hasTitle', 'merged title');
        $resp  = $this->updateResource($meta2);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp->getBody(), $location);

        $this->assertEquals('test.ttl', (string) $meta3->getLiteral(self::$config->schema->fileName));
        $this->assertEquals(1, count($meta3->all('http://test/hasTitle')));
        $this->assertEquals('merged title', (string) $meta3->getLiteral('http://test/hasTitle'));
        $this->assertEquals(2, count($meta3->all(self::$config->schema->id)));
        $ids = array_map(function ($x) {
            return (string) $x;
        }, $meta3->all(self::$config->schema->id));
        $this->assertContains((string) $meta1->get(self::$config->schema->id), $ids);
        $this->assertContains('https://123', $ids);
    }

    /**
     * @group rest
     */
    public function testPatchMetadataAdd(): void {
        $location = $this->createBinaryResource();
        $meta1    = $this->getResourceMeta($location);
        $meta1->addLiteral('http://test/hasTitle', 'foo bar');
        $this->updateResource($meta1);

        $g     = new Graph();
        $meta2 = $g->resource($location);
        $meta2->addResource(self::$config->schema->id, 'https://123');
        $meta2->addLiteral('http://test/hasTitle', 'merged title');
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_ADD);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp->getBody(), $location);

        $this->assertEquals('test.ttl', (string) $meta3->getLiteral(self::$config->schema->fileName));
        $this->assertEquals(2, count($meta3->all('http://test/hasTitle')));
        $titles = array_map(function ($x) {
            return (string) $x;
        }, $meta3->all('http://test/hasTitle'));
        $this->assertContains('foo bar', $titles);
        $this->assertContains('merged title', $titles);
        $ids = array_map(function ($x) {
            return (string) $x;
        }, $meta3->all(self::$config->schema->id));
        $this->assertContains((string) $meta1->get(self::$config->schema->id), $ids);
        $this->assertContains('https://123', $ids);
    }

    public function testPatchMetadataDelProp(): void {
        $meta1    = (new Graph())->resource(self::$baseUrl);
        $meta1->addLiteral('https://my/prop', 'my value');
        $location = $this->createMetadataResource($meta1);

        $meta2 = $this->getResourceMeta($location);
        $this->assertEquals('my value', $meta2->getLiteral('https://my/prop'));

        $meta2->delete('https://my/prop');
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_MERGE);
        $meta3 = $this->extractResource($resp, $location);
        $this->assertEquals('my value', $meta3->getLiteral('https://my/prop'));

        $meta2->addResource(self::$config->schema->delete, 'https://my/prop');
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_MERGE);
        $meta4 = $this->extractResource($resp, $location);
        $this->assertNull($meta4->getLiteral('https://my/prop'));
    }

    /**
     * @group rest
     */
    public function testPatchMetadataWrongMode(): void {
        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $resp     = $this->updateResource($meta, null, 'foo');
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Wrong metadata merge mode foo', (string) $resp->getBody());
    }

    public function testPatchWrongTransactionId(): void {
        $location = $this->createBinaryResource();
        $headers  = [
            self::$config->rest->headers->transactionId => -123,
            'Content-Type'                              => 'application/n-triples',
            'Eppn'                                      => 'admin',
        ];
        $req      = new Request('patch', $location . '/metadata', $headers);
        $resp     = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Begin transaction first', (string) $resp->getBody());
    }

    /**
     * @group rest
     */
    public function testDuplicatedId(): void {
        $res1  = $this->createMetadataResource((new Graph())->resource(self::$baseUrl));
        $meta1 = $this->getResourceMeta($res1);
        $meta1->addResource(self::$config->schema->id, 'https://my.id');
        $resp  = $this->updateResource($meta1);
        $this->assertEquals(200, $resp->getStatusCode());

        $res2  = $this->createMetadataResource((new Graph())->resource(self::$baseUrl));
        $meta2 = $this->getResourceMeta($res2);
        $meta2->addResource(self::$config->schema->id, 'https://my.id');
        $resp  = $this->updateResource($meta2);
        $this->assertEquals(409, $resp->getStatusCode());
        $this->assertEquals('Duplicated resource identifier', (string) $resp->getBody());
    }

    /**
     * @group rest
     */
    public function testUnbinaryResource(): void {
        $location = $this->createBinaryResource();
        $txHeader = self::$config->rest->headers->transactionId;

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $txId = $this->beginTransaction();

        $req  = new Request('put', $location, $this->getHeaders($txId), '');
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertEquals($location . '/metadata', $resp->getHeader('Location')[0]);

        $this->commitTransaction($txId);

        $resp = self::$client->send($req->withoutHeader($txHeader));
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertEquals($location . '/metadata', $resp->getHeader('Location')[0]);

        $req  = new Request('get', $location . '/metadata');
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $res  = $this->extractResource($resp, $location);
        $this->assertNull($res->getLiteral(self::$config->schema->fileName));
        $this->assertNull($res->getLiteral(self::$config->schema->binarySize));
        $this->assertNull($res->getLiteral(self::$config->schema->hash));
    }

    /**
     * @group rest
     */
    public function testEmptyMeta(): void {
        $location = $this->createBinaryResource();

        $txId = $this->beginTransaction();
        $req  = new Request('patch', $location . '/metadata', $this->getHeaders($txId), '');
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->commitTransaction($txId);
    }

    public function testGetNone(): void {
        $location                                                = $this->createBinaryResource();
        $headers                                                 = $this->getHeaders();
        $headers[self::$config->rest->headers->metadataReadMode] = RRI::META_NONE;
        $req                                                     = new Request('get', "$location/metadata", $headers);
        $resp                                                    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $this->assertEmpty((string) $resp->getBody());
    }

    public function testGetRelatives(): void {
        $txId = $this->beginTransaction();
        $m    = [
            $this->getResourceMeta($this->createBinaryResource($txId)),
            $this->getResourceMeta($this->createBinaryResource($txId)),
            $this->getResourceMeta($this->createBinaryResource($txId)),
        ];
        $m[0]->addResource('https://relation', $m[1]->getUri());
        $this->updateResource($m[0], $txId);
        $m[1]->addResource('https://relation', $m[2]->getUri());
        $this->updateResource($m[1], $txId);
        $this->commitTransaction($txId);

        $headers = [
            self::$config->rest->headers->metadataReadMode       => RRI::META_RELATIVES,
            self::$config->rest->headers->metadataParentProperty => 'https://relation',
        ];
        $req     = new Request('get', $m[0]->getUri() . '/metadata', $headers);
        $resp    = self::$client->send($req);
        $g       = new Graph();
        $g->parse((string) $resp->getBody());
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertGreaterThan(0, count($g->resource($m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($m[2]->getUri())->propertyUris()));
    }

    public function testMerge(): void {
        $idProp                                                  = self::$config->schema->id;
        $txId                                                    = $this->beginTransaction();
        $headers                                                 = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->metadataReadMode] = RRI::META_RESOURCE;

        $meta1 = (new Graph())->resource(self::$baseUrl);
        $meta1->addResource($idProp, 'http://res1');
        $meta1->addLiteral('http://foo', '1');
        $meta1->addLiteral('http://bar', '2');
        $meta2 = (new Graph())->resource(self::$baseUrl);
        $meta2->addResource($idProp, 'http://res2');
        $meta2->addLiteral('http://bar', 'A');
        $meta2->addLiteral('http://baz', 'B');

        $loc1 = $this->createMetadataResource($meta1, $txId);
        $loc2 = $this->createMetadataResource($meta2, $txId);
        $id1  = substr($loc1, strlen(self::$baseUrl));
        $id2  = substr($loc2, strlen(self::$baseUrl));

        $req  = new Request('put', self::$baseUrl . "merge/$id2/$id1", $headers);
        $resp = self::$client->send($req);

        $g       = new Graph();
        $g->parse((string) $resp->getBody());
        $this->assertEquals(200, $resp->getStatusCode());
        $meta    = new Graph();
        $meta->parse($resp->getBody());
        $metaRes = $meta->resource($loc1);
        // all ids are preserved
        $ids     = [];
        foreach ($metaRes->allResources($idProp) as $i) {
            $ids[] = (string) $i;
        }
        $this->assertContains($loc1, $ids);
        $this->assertContains($loc2, $ids);
        $this->assertContains('http://res1', $ids);
        $this->assertContains('http://res2', $ids);
        // unique properties are preserved, common are kept from target
        $this->assertCount(1, $metaRes->all('http://foo'));
        $this->assertCount(1, $metaRes->all('http://bar'));
        $this->assertCount(1, $metaRes->all('http://baz'));
        $this->assertEquals('1', (string) $metaRes->get('http://foo'));
        $this->assertEquals('2', (string) $metaRes->get('http://bar'));
        $this->assertEquals('B', (string) $metaRes->get('http://baz'));

        $resp = self::$client->send(new Request('get', "$loc1/metadata"));
        $this->assertEquals(200, $resp->getStatusCode());
        $resp = self::$client->send(new Request('get', $loc2));
        $this->assertEquals(404, $resp->getStatusCode());

        $this->commitTransaction($txId);

        $resp = self::$client->send(new Request('get', "$loc1/metadata"));
        $this->assertEquals(200, $resp->getStatusCode());
        $resp = self::$client->send(new Request('get', $loc2));
        $this->assertEquals(404, $resp->getStatusCode());
    }

    public function testBadMetaMethod(): void {
        $location = $this->createBinaryResource();
        $headers  = [
            self::$config->rest->headers->metadataReadMode => 'foo',
        ];
        $req      = new Request('get', $location . '/metadata', $headers);
        $resp     = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Bad metadata mode foo', (string) $resp->getBody());
    }

    public function testSingleModDate(): void {
        $location = $this->createBinaryResource();
        $resp     = $this->updateResource((new Graph())->resource($location));
        $this->assertEquals(200, $resp->getStatusCode());
        $meta     = $this->extractResource($resp, $location);
        $this->assertEquals(1, count($meta->all(self::$config->schema->modificationDate)));
        $this->assertEquals(1, count($meta->all(self::$config->schema->modificationUser)));
    }

    /**
     * @group rest
     */
    public function testMethodNotAllowed(): void {
        $req  = new Request('put', self::$baseUrl);
        $resp = self::$client->send($req);
        $this->assertEquals(405, $resp->getStatusCode());
    }

    /**
     * @group rest
     */
    function testPseudoDuplicate(): void {
        $txId   = $this->beginTransaction();
        $idProp = self::$config->schema->id;
        $prop   = 'https://bar/baz';

        $meta      = (new Graph())->resource(self::$baseUrl);
        $meta->addResource($idProp, 'https://foo/bar1');
        $meta->addResource($idProp, 'https://foo/bar2');
        $location1 = $this->createMetadataResource($meta, $txId);

        $meta      = (new Graph())->resource(self::$baseUrl);
        $meta->addResource($idProp, 'https://foo/baz');
        $meta->addResource($prop, 'https://foo/bar1');
        $meta->addResource($prop, 'https://foo/bar2');
        $location2 = $this->createMetadataResource($meta, $txId);
        $this->assertIsString($location2);

        $req  = new Request('get', $location2 . '/metadata');
        $resp = self::$client->send($req);
        $g    = new Graph();
        $g->parse((string) $resp->getBody());
        $r    = $g->resource($location2);
        $this->assertEquals(1, count($r->all($prop)));
        $this->assertEquals($location1, (string) $r->get($prop));

        $this->rollbackTransaction($txId);
    }

    /**
     * @group rest
     */
    public function testAutoAddIds(): void {
        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);

        $cfg                                                      = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['metadataManagment']['autoAddIds']['default']        = 'deny';
        $cfg['metadataManagment']['autoAddIds']['denyNamespaces'] = ['https://deny.nmsp'];
        $cfg['metadataManagment']['autoAddIds']['skipNamespaces'] = ['https://skip.nmsp'];
        $cfg['metadataManagment']['autoAddIds']['addNamespaces']  = ['https://add.nmsp'];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $meta->addResource('https://some.property/1', 'https://skip.nmsp/123');
        $meta->addResource('https://some.property/2', 'https://add.nmsp/123');
        $resp  = $this->updateResource($meta);
        $this->assertEquals(200, $resp->getStatusCode());
        $m     = $this->extractResource($resp, $location);
        $g     = $m->getGraph();
        $this->assertEquals(0, count($g->resourcesMatching(self::$config->schema->id, new Resource('https://skip.nmsp/123'))));
        $this->assertNull($m->getResource('https://some.property/1'));
        $this->assertEquals(1, count($g->resourcesMatching(self::$config->schema->id, new Resource('https://add.nmsp/123'))));
        $added = $g->resourcesMatching(self::$config->schema->id, new Resource('https://add.nmsp/123'))[0];
        $this->assertEquals($added->getUri(), (string) $m->getResource('https://some.property/2'));

        // and deny
        $meta->addResource('https://some.property/2', 'https://deny.nmsp/123');
        $resp = $this->updateResource($meta);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Denied to create a non-existing id', (string) $resp->getBody());
    }

    /**
     * @group rest
     */
    public function testWrongIds(): void {
        $txId    = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];

        $res  = (new Graph())->resource(self::$baseUrl);
        $res->addResource(self::$config->schema->id, self::$baseUrl . '0');
        $req  = new Request('post', self::$baseUrl . 'metadata', $headers, $res->getGraph()->serialise('text/turtle'));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertMatchesRegularExpression('/^Id in the repository base URL namespace which does not match the resource id/', (string) $resp->getBody());

        $res  = (new Graph())->resource(self::$baseUrl);
        $res->addLiteral(self::$config->schema->id, self::$baseUrl . '0');
        $req  = new Request('post', self::$baseUrl . 'metadata', $headers, $res->getGraph()->serialise('text/turtle'));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Non-resource identifier', (string) $resp->getBody());
    }

    /**
     * @group rest
     */
    function testVeryOldDate(): void {
        $meta     = (new Graph())->resource(self::$baseUrl);
        $meta->addLiteral('https://old/date1', new Literal('-12345-01-01', null, RDF::XSD_DATE));
        $meta->addLiteral('https://old/date2', new Literal('-4713-01-01', null, RDF::XSD_DATE));
        $meta->addLiteral('https://old/date3', new Literal('-4714-01-01', null, RDF::XSD_DATE));
        $location = $this->createMetadataResource($meta);
        $req      = new Request('get', $location . '/metadata');
        $resp     = self::$client->send($req);
        $g        = new Graph();
        $g->parse((string) $resp->getBody());
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('-12345-01-01', (string) $g->resource($location)->get('https://old/date1'));
        $this->assertEquals('-4713-01-01', (string) $g->resource($location)->get('https://old/date2'));
        $this->assertEquals('-4714-01-01', (string) $g->resource($location)->get('https://old/date3'));
    }

    /**
     * @group rest
     */
    function testWrongValue(): void {
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addLiteral('https://wrong/date', new Literal('foo', null, RDF::XSD_DATE));
        try {
            $this->createMetadataResource($meta);
            $this->assertTrue(false);
        } catch (RuntimeException $e) {
            $this->assertStringContainsString('Wrong property value', $e->getMessage());
        }
    }

    /**
     * @group rest
     */
    public function testSpatial(): void {
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
        ];

        // geojson
        $headers['Content-Disposition'] = 'attachment; filename="test.geojson"';
        $headers['Content-Type']        = 'application/geo+json';
        $body                           = '{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 3]}}]}';
        $resp                           = self::$client->send(new Request('post', self::$baseUrl, $headers, $body));
        $this->assertEquals(201, $resp->getStatusCode());
        $location                       = $resp->getHeader('Location')[0];
        $id                             = preg_replace('|^.*/|', '', $location);
        $query                          = self::$pdo->prepare("SELECT count(*) FROM spatial_search WHERE id = ? AND geom && st_setsrid(st_point(1, 2), 4326)::geography");
        $query->execute([$id]);
        $this->assertEquals(1, $query->fetchColumn());

        // kml
        $headers['Content-Disposition'] = 'attachment; filename="test.kml"';
        $headers['Content-Type']        = 'application/vnd.google-earth.kml+xml';
        $body                           = (string) file_get_contents(__DIR__ . '/data/test.kml');
        $resp                           = self::$client->send(new Request('post', self::$baseUrl, $headers, $body));
        $this->assertEquals(201, $resp->getStatusCode());
        $location                       = $resp->getHeader('Location')[0];
        $id                             = preg_replace('|^.*/|', '', $location);
        $query                          = self::$pdo->prepare("SELECT count(*) FROM spatial_search WHERE id = ? AND geom && st_setsrid(st_point(0, 0), 4326)::geography");
        $query->execute([$id]);
        $this->assertEquals(1, $query->fetchColumn());

        // gml
        $headers['Content-Disposition'] = 'attachment; filename="test.gml"';
        $headers['Content-Type']        = 'application/gml+xml; version=3.2';
        $body                           = (string) file_get_contents(__DIR__ . '/data/test.gml');
        $resp                           = self::$client->send(new Request('post', self::$baseUrl, $headers, $body));
        $this->assertEquals(201, $resp->getStatusCode());
        $location                       = $resp->getHeader('Location')[0];
        $id                             = preg_replace('|^.*/|', '', $location);
        $query                          = self::$pdo->prepare("SELECT count(*) FROM spatial_search WHERE id = ? AND geom && st_setsrid(st_point(0, 0), 4326)::geography");
        $query->execute([$id]);
        $this->assertEquals(1, $query->fetchColumn());

        // geoTIFF
        $headers['Content-Disposition'] = 'attachment; filename="test.tif"';
        $headers['Content-Type']        = 'image/tiff';
        $body                           = (string) file_get_contents(__DIR__ . '/data/georaster.tif');
        $resp                           = self::$client->send(new Request('post', self::$baseUrl, $headers, $body));
        $this->assertEquals(201, $resp->getStatusCode());
        $location                       = $resp->getHeader('Location')[0];
        $id                             = preg_replace('|^.*/|', '', $location);
        $query                          = self::$pdo->prepare("SELECT count(*) FROM spatial_search WHERE id = ? AND geom && st_setsrid(st_point(102.549, 17.572), 4326)::geography");
        $query->execute([$id]);
        $this->assertEquals(1, $query->fetchColumn());

        // ordinary tif - shouldn't rise an error
        $headers['Content-Disposition'] = 'attachment; filename="test.tif"';
        $headers['Content-Type']        = 'image/tiff';
        $body                           = (string) file_get_contents(__DIR__ . '/data/raster.tif');
        $req                            = new Request('post', self::$baseUrl, $headers, $body);
        $resp                           = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location                       = $resp->getHeader('Location')[0];
        $id                             = preg_replace('|^.*/|', '', $location);
        $query                          = self::$pdo->prepare("SELECT count(*) FROM spatial_search WHERE id = ?");
        $query->execute([$id]);
        $this->assertEquals(0, $query->fetchColumn());

        // metadata
        $meta     = (new Graph())->resource(self::$baseUrl);
        $meta->addResource(self::$config->schema->id, 'https://' . rand());
        $meta->addLiteral('http://test/hasTitle', 'title');
        $meta->addLiteral(self::$config->spatialSearch->properties[0], 'POLYGON((0 0,0 10,10 10,10 0,0 0))');
        $headers  = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req      = new Request('post', self::$baseUrl . 'metadata', $headers, $meta->getGraph()->serialise('application/n-triples'));
        $resp     = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0];
        $id       = preg_replace('|^.*/|', '', $location);
        $query    = self::$pdo->prepare("
            SELECT count(*) FROM spatial_search JOIN metadata m USING (mid) 
            WHERE m.id = ? AND geom && st_setsrid(st_point(5, 5), 4326)::geography
        ");
        $query->execute([$id]);
        $this->assertEquals(1, $query->fetchColumn());

        $this->rollbackTransaction($txId);
    }

    /**
     * In the current implementation only a single request can use a given transaction.
     * Another request trying to use the same transaction in parallel should gracefully
     * return HTTP 409.
     * 
     * @group rest
     */
    public function testParallelRequests(): void {
        $location = $this->createMetadataResource();
        $prop     = 'http://foo';
        $txId     = $this->beginTransaction();
        $headers  = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Type'                              => 'application/n-triples',
        ];
        $req1     = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value1\" .");
        $req2     = new Request('patch', $location . '/metadata', $headers, "<$location> <$prop> \"value2\" .");
        $prom1    = self::$client->sendAsync($req1);
        $prom2    = self::$client->sendAsync($req2);
        $resp1    = $prom1->wait();
        $resp2    = $prom2->wait();
        $codes    = [$resp1->getStatusCode(), $resp2->getStatusCode()];
        $this->assertContains(200, $codes);
        $this->assertContains(409, $codes);
    }

    public function testVariousReadModes(): void {
        $location  = $this->createMetadataResource();
        $readModes = [RRI::META_IDS, RRI::META_NEIGHBORS, RRI::META_PARENTS,
            RRI::META_PARENTS_ONLY, RRI::META_PARENTS_REVERSE, RRI::META_RELATIVES,
            RRI::META_RELATIVES_ONLY, RRI::META_RELATIVES_REVERSE, RRI::META_RESOURCE];
        foreach ($readModes as $mode) {
            $req  = new Request('get', $location . '/metadata?readMode=' . rawurldecode($mode));
            $resp = self::$client->send($req);
            $this->assertEquals(200, $resp->getStatusCode());
        }

        $req  = new Request('get', $location . '/metadata?readMode=' . rawurldecode(RRI::META_NONE));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
    }
}
