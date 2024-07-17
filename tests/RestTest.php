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
use GuzzleHttp\Psr7\Request;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\Util as RdfIoUtil;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\Metadata;
use acdhOeaw\arche\core\BinaryPayload;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;

/**
 * Description of RestTest
 *
 * @author zozlak
 */
class RestTest extends TestBase {

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
        $metaN1   = new DatasetNode(DF::namedNode($location));
        $metaN1->add(RdfIoUtil::parse($resp, new DF()));

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals($body, $resp->getBody(), 'created file content mismatch');

        $req    = new Request('get', $location . '/metadata', $this->getHeaders($txId));
        $resp   = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $metaN2 = new DatasetNode(DF::namedNode($location));
        $metaN2->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals('md5:' . md5_file(__DIR__ . '/data/test.ttl'), $metaN2->getObjectValue(new PT(self::$schema->hash)));
        $this->assertTrue($metaN1->equals($metaN2));

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
            $meta              = new DatasetNode(DF::namedNode($location));
            $meta->add(RdfIoUtil::parse($resp, new DF()));
            if ($prevMeta !== null) {
                $this->assertTrue($prevMeta->equals($meta));
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

    public function testResourceDelete(): void {
        $location = $this->createBinaryResource();

        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('delete', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new DatasetNode(DF::namedNode($location));
        $meta->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals($location, $meta->getObjectValue(new PT(self::$schema->id)));

        $req  = new Request('get', $location, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());

        $this->assertEquals(204, $this->commitTransaction($txId));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(410, $resp->getStatusCode());
    }

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

    public function testTombstoneDeleteActive(): void {
        $location = $this->createBinaryResource();

        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);
        $req  = new Request('delete', $location . '/tombstone', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(405, $resp->getStatusCode());

        $this->rollbackTransaction($txId);
    }

    public function testDeleteRecursively(): void {
        $txId = $this->beginTransaction();

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quadNoSubject(self::$schema->parent, DF::namedNode($loc1)));
        $loc2 = $this->createMetadataResource($meta, $txId);

        $headers                                                       = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->metadataParentProperty] = self::$config->schema->parent;

        $req     = new Request('delete', $loc1, $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta    = new Dataset();
        $meta->add(RdfIoUtil::parse($resp, new DF()));
        $deleted = $meta->listObjects(new PT(self::$schema->id))->getValues();
        foreach ($deleted as $delres) {
            $resp = self::$client->send(new Request('get', $delres));
            $this->assertEquals(410, $resp->getStatusCode());
        }
        $this->assertContains($loc1, $deleted);
        $this->assertContains($loc2, $deleted);

        $this->commitTransaction($txId);

        foreach ($deleted as $delres) {
            $resp = self::$client->send(new Request('get', $delres));
            $this->assertEquals(410, $resp->getStatusCode());
        }
    }

    public function testDeleteWithReferences(): void {
        $txId    = $this->beginTransaction();
        $headers = $this->getHeaders($txId);

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quadNoSubject(self::$schema->parent, df::namedNode($loc1)));
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
        $meta = new DatasetNode(DF::namedNode($loc1));
        $meta->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertFalse($meta->any(new PT('http://relation')));
    }

    public function testForeignCheckSeparateTx(): void {
        $txId = $this->beginTransaction();
        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quadNoSubject(self::$schema->parent, DF::namedNode($loc1)));
        $this->createMetadataResource($meta, $txId);
        $this->commitTransaction($txId);

        $txId = $this->beginTransaction();
        $req  = new Request('delete', $loc1, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(409, $resp->getStatusCode());
    }

    public function testForeignCheckSameTx(): void {
        $txId = $this->beginTransaction();

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quadNoSubject(self::$schema->parent, DF::namedNode($loc1)));
        $this->createMetadataResource($meta, $txId);

        $req  = new Request('delete', $loc1, $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(409, $resp->getStatusCode());
    }

    public function testHead(): void {
        $location = $this->createBinaryResource();

        $req  = new Request('head', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('attachment; filename="test.ttl"', $resp->getHeader('Content-Disposition')[0] ?? '');
        $this->assertEquals('text/turtle;charset=UTF-8', $resp->getHeader('Content-Type')[0] ?? '');
        // In HTTP/1.1 and newer server may respond with transfer-encoding: chuncked which does not contain the content-length header
        if (count($resp->getHeader('Content-Length')) > 0) {
            $this->assertEquals(541, $resp->getHeader('Content-Length')[0] ?? '');
        }

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
        $this->assertEquals('RestTest.php', $meta->getObjectValue(new PT(self::$schema->fileName)));
        $this->assertEquals('application/php', $meta->getObjectValue(new PT(self::$schema->mime)));

        $this->rollbackTransaction($txId);
    }

    public function testResourceCreateMetadata(): void {
        $idTmpl = new PT(self::$schema->id);

        $txId = $this->beginTransaction();

        $meta    = $this->createMetadata();
        $headers = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req     = new Request('post', self::$baseUrl . 'metadata', $headers, self::$serializer->serialize($meta));
        $resp    = self::$client->send($req);

        $this->assertEquals(201, $resp->getStatusCode());

        $location = $resp->getHeader('Location')[0];
        $metaN1   = new DatasetNode(DF::namedNode($location));
        $metaN1->add(RdfIoUtil::parse($resp, new DF()));

        $req  = new Request('get', $location, $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertEquals($location . '/metadata', $resp->getHeader('Location')[0]);

        $req     = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp    = self::$client->send($req);
        $body    = $resp->getBody();
        $this->assertEquals(200, $resp->getStatusCode());
        $metaN2  = new DatasetNode(DF::namedNode($location));
        $metaN2->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertTrue($metaN1->equals($metaN2));
        $this->assertCount(2, $metaN1->copy($idTmpl));
        $ids     = $metaN1->listObjects($idTmpl)->getValues();
        sort($ids);
        $allowed = array_merge([$location], $meta->listObjects($idTmpl)->getValues());
        sort($allowed);
        $this->assertEquals($allowed, $ids);

        $this->assertMatchesRegularExpression('|^' . self::$baseUrl . '[0-9]+$|', (string) $metaN1->getObjectValue(new PT('http://test/hasRelation')));
        $this->assertEquals('title', $metaN1->getObjectValue(new PT('http://test/hasTitle')));
        $this->assertEquals(date('Y-m-d'), substr((string) $metaN1->getObjectValue(new PT('http://test/hasDate')), 0, 10));
        $this->assertEquals(123.5, $metaN1->getObjectValue(new PT('http://test/hasNumber')));

        $this->assertEquals(204, $this->commitTransaction($txId));

        // check if everything is still in place after the transaction end
        $req  = new Request('get', $location . '/metadata', $this->getHeaders());
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals((string) $body, (string) $resp->getBody());
    }

    public function testPatchMetadataMerge(): void {
        $titleTmpl = new PT('http://test/hasTitle');
        $idTmpl    = new PT(self::$schema->id);
        $location  = $this->createBinaryResource();
        $meta1     = $this->getResourceMeta($location);

        $meta2 = new DatasetNode(DF::namedNode($location));
        $meta2->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://123')),
            DF::quadNoSubject(DF::namedNode('http://test/hasTitle'), DF::literal('merged title')),
        ]);
        $resp  = $this->updateResource($meta2);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp->getBody(), $location);

        $this->assertEquals('test.ttl', $meta3->getObjectValue(new PT(self::$schema->fileName)));
        $this->assertEquals(['merged title'], $meta3->listObjects($titleTmpl)->getValues());
        $ids = $meta3->listObjects($idTmpl)->getValues();
        $this->assertCount(2, $ids);
        $this->assertContains($meta1->getObjectValue($idTmpl), $ids);
        $this->assertContains('https://123', $ids);
    }

    public function testPatchMetadataAdd(): void {
        $titleProp = DF::namedNode('http://test/hasTitle');
        $idTmpl    = new PT(self::$schema->id);
        $location  = $this->createBinaryResource();
        $meta1     = $this->getResourceMeta($location);
        $meta1->add(DF::quadNoSubject($titleProp, DF::literal('foo bar')));
        $this->updateResource($meta1);

        $meta2 = new DatasetNode(DF::namedNode($location));
        $meta2->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://123')),
            DF::quadNoSubject($titleProp, DF::literal('merged title')),
        ]);
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_ADD);
        $this->assertEquals(200, $resp->getStatusCode());
        $meta3 = $this->extractResource($resp->getBody(), $location);

        $this->assertEquals('test.ttl', $meta3->getObjectValue(new PT(self::$schema->fileName)));
        $titles = $meta3->listObjects(new PT($titleProp))->getValues();
        sort($titles);
        $this->assertEquals(['foo bar', 'merged title'], $titles);
        $ids    = $meta3->listObjects($idTmpl)->getValues();
        sort($ids);
        $this->assertContains($meta1->getObjectValue($idTmpl), $ids);
        $this->assertContains('https://123', $ids);
    }

    public function testPatchMetadataDelProp(): void {
        $prop     = DF::namedNode('https://my/prop');
        $propTmpl = new PT($prop);

        $meta1    = new DatasetNode(self::$baseNode);
        $meta1->add(DF::quadNoSubject($prop, DF::literal('my value')));
        $location = $this->createMetadataResource($meta1);

        $meta2 = $this->getResourceMeta($location);
        $this->assertEquals('my value', $meta2->getObjectValue($propTmpl));

        $meta2->delete(new PT($prop));
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_MERGE);
        $meta3 = $this->extractResource($resp, $location);
        $this->assertEquals('my value', $meta3->getObjectValue($propTmpl));

        $meta2->add(DF::quadNoSubject(self::$schema->delete, $prop));
        $resp  = $this->updateResource($meta2, null, Metadata::SAVE_MERGE);
        $meta4 = $this->extractResource($resp, $location);
        $this->assertNull($meta4->getObjectValue($propTmpl));
    }

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

    public function testDuplicatedId(): void {
        $res1  = $this->createMetadataResource();
        $meta1 = $this->getResourceMeta($res1);
        $meta1->add(DF::quadNoSubject(self::$schema->id, DF::namedNode('https://my.id')));
        $resp  = $this->updateResource($meta1);
        $this->assertEquals(200, $resp->getStatusCode());

        $res2  = $this->createMetadataResource();
        $meta2 = $this->getResourceMeta($res2);
        $meta2->add(DF::quadNoSubject(self::$schema->id, DF::namedNode('https://my.id')));
        $resp  = $this->updateResource($meta2);
        $this->assertEquals(409, $resp->getStatusCode());
        $this->assertEquals('Duplicated resource identifier', (string) $resp->getBody());
    }

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
        $this->assertFalse($res->any(new PT(self::$schema->fileName)));
        $this->assertFalse($res->any(new PT(self::$schema->binarySize)));
        $this->assertFalse($res->any(new PT(self::$schema->hash)));
    }

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
        $m[0]->add(DF::quadNoSubject(self::$schema->parent, $m[1]->getNode()));
        $this->updateResource($m[0], $txId);
        $m[1]->add(DF::quadNoSubject(self::$schema->parent, $m[2]->getNode()));
        $this->updateResource($m[1], $txId);
        $this->commitTransaction($txId);

        $headers = [
            self::$config->rest->headers->metadataReadMode       => RRI::META_RELATIVES,
            self::$config->rest->headers->metadataParentProperty => self::$schema->parent->getValue(),
        ];
        $req     = new Request('get', $m[0]->getNode()->getValue() . '/metadata', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $g       = new Dataset();
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertTrue($g->any(new QT($m[0]->getNode())));
        $this->assertTrue($g->any(new QT($m[1]->getNode())));
        $this->assertTrue($g->any(new QT($m[2]->getNode())));
    }

    public function testMerge(): void {
        $fooProp                                                 = DF::namedNode('http://foo');
        $barProp                                                 = DF::namedNode('http://bar');
        $bazProp                                                 = DF::namedNode('http://baz');
        $fooTmpl                                                 = new PT($fooProp);
        $barTmpl                                                 = new PT($barProp);
        $bazTmpl                                                 = new PT($bazProp);
        $txId                                                    = $this->beginTransaction();
        $headers                                                 = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->metadataReadMode] = RRI::META_RESOURCE;

        $meta1 = new DatasetNode(self::$baseNode);
        $meta1->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('http://res1')),
            DF::quadNoSubject($fooProp, DF::literal('1')),
            DF::quadNoSubject($barProp, DF::literal('2')),
        ]);
        $meta2 = new DatasetNode(self::$baseNode);
        $meta2->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('http://res2')),
            DF::quadNoSubject($barProp, DF::literal('A')),
            DF::quadNoSubject($bazProp, DF::literal('B')),
        ]);

        $loc1 = $this->createMetadataResource($meta1, $txId);
        $loc2 = $this->createMetadataResource($meta2, $txId);
        $id1  = substr($loc1, strlen(self::$baseUrl));
        $id2  = substr($loc2, strlen(self::$baseUrl));

        $req  = new Request('put', self::$baseUrl . "merge/$id2/$id1", $headers);
        $resp = self::$client->send($req);

        $this->assertEquals(200, $resp->getStatusCode());
        $meta = new DatasetNode(DF::namedNode($loc1));
        $meta->add(RdfIoUtil::parse($resp, new DF()));
        // all ids are preserved
        $ids  = $meta->listObjects(new PT(self::$schema->id))->getValues();
        $this->assertContains($loc1, $ids);
        $this->assertNotContains($loc2, $ids);
        $this->assertContains('http://res1', $ids);
        $this->assertContains('http://res2', $ids);
        // unique properties are preserved, common are kept from target
        $this->assertEquals(['1'], $meta->listObjects($fooTmpl)->getValues());
        $this->assertEquals(['2'], $meta->listObjects($barTmpl)->getValues());
        $this->assertEquals(['B'], $meta->listObjects($bazTmpl)->getValues());

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

    /**
     * @see https://github.com/acdh-oeaw/arche-core/issues/28
     * @return void
     */
    public function testMergeRollback(): void {
        $barProp = DF::namedNode('http://bar');
        $barTmpl = new PT($barProp);
        $idsTmpl = new PT(self::$schema->id);
        $meta1   = new DatasetNode(self::$baseNode);
        $meta1->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('http://res1')),
            DF::quadNoSubject($barProp, DF::literal('1')),
        ]);
        $meta2   = new DatasetNode(self::$baseNode);
        $meta2->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('http://res2')),
            DF::quadNoSubject($barProp, DF::literal('A')),
        ]);
        $loc1    = $this->createMetadataResource($meta1);
        $loc2    = $this->createMetadataResource($meta2);
        $id1     = substr($loc1, strlen(self::$baseUrl));
        $id2     = substr($loc2, strlen(self::$baseUrl));

        // as the https://github.com/acdh-oeaw/arche-core/issues/28 depends on
        // the order of rows returned by the database query try different
        // resource merging order and repeat the test a few times
        foreach ([0, 1] as $order) {
            $mergeUrl = $order ? "$id2/$id1" : "$id1/$id2";
            for ($i = 0; $i < 3; $i++) {
                $txId                                                    = $this->beginTransaction();
                $headers                                                 = $this->getHeaders($txId);
                $headers[self::$config->rest->headers->metadataReadMode] = RRI::META_RESOURCE;

                $req  = new Request('put', self::$baseUrl . "merge/$mergeUrl", $headers);
                $resp = self::$client->send($req);
                $this->assertEquals(200, $resp->getStatusCode());
                $resp = self::$client->send(new Request('get', "$loc1/metadata"));
                $this->assertEquals($order ? 200 : 404, $resp->getStatusCode(), "$mergeUrl");
                $resp = self::$client->send(new Request('get', "$loc2/metadata"));
                $this->assertEquals($order ? 404 : 200, $resp->getStatusCode(), "$mergeUrl");

                $this->assertEquals(204, $this->rollbackTransaction($txId));

                $resp    = self::$client->send(new Request('get', "$loc1/metadata"));
                $this->assertEquals(200, $resp->getStatusCode());
                $metaRes = new DatasetNode(DF::namedNode($loc1));
                $metaRes->add(RdfIoUtil::parse($resp, new DF()));
                $ids     = $metaRes->listObjects($idsTmpl)->getValues();
                $this->assertContains('http://res1', $ids);
                $this->assertEquals('1', $metaRes->getObjectValue($barTmpl));

                $resp    = self::$client->send(new Request('get', "$loc2/metadata"));
                $this->assertEquals(200, $resp->getStatusCode());
                $metaRes = new DatasetNode(DF::namedNode($loc2));
                $metaRes->add(RdfIoUtil::parse($resp, new DF()));
                $ids     = $metaRes->listObjects($idsTmpl)->getValues();
                $this->assertContains('http://res2', $ids);
                $this->assertEquals('A', $metaRes->getObjectValue($barTmpl));
            }
        }
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
        $resp     = $this->updateResource(new DatasetNode(DF::namedNode($location)));
        $this->assertEquals(200, $resp->getStatusCode());
        $meta     = $this->extractResource($resp, $location);
        $this->assertCount(1, $meta->copy(new PT(self::$schema->modificationDate)));
        $this->assertCount(1, $meta->copy(new PT(self::$schema->modificationUser)));
    }

    public function testMethodNotAllowed(): void {
        $req  = new Request('put', self::$baseUrl);
        $resp = self::$client->send($req);
        $this->assertEquals(405, $resp->getStatusCode());
    }

    function testPseudoDuplicate(): void {
        $txId = $this->beginTransaction();
        $prop = DF::namedNode('https://bar/baz');

        $meta      = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://foo/bar1')),
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://foo/bar2')),
        ]);
        $location1 = $this->createMetadataResource($meta, $txId);

        $meta      = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://foo/baz')),
            DF::quadNoSubject($prop, DF::namedNode('https://foo/bar1')),
            DF::quadNoSubject($prop, DF::namedNode('https://foo/bar2')),
        ]);
        $location2 = $this->createMetadataResource($meta, $txId);
        $this->assertIsString($location2);

        $req  = new Request('get', $location2 . '/metadata');
        $resp = self::$client->send($req);
        $r    = new DatasetNode(DF::namedNode($location2));
        $r->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals([$location1], $r->listObjects(new PT($prop))->getValues());

        $this->rollbackTransaction($txId);
    }

    public function testAutoAddIds(): void {
        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $skipNode = DF::namedNode('https://skip.nmsp/123');
        $addNode  = DF::namedNode('https://add.nmsp/123');

        $cfg                                                      = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['metadataManagment']['autoAddIds']['default']        = 'deny';
        $cfg['metadataManagment']['autoAddIds']['denyNamespaces'] = ['https://deny.nmsp'];
        $cfg['metadataManagment']['autoAddIds']['skipNamespaces'] = ['https://skip.nmsp'];
        $cfg['metadataManagment']['autoAddIds']['addNamespaces']  = ['https://add.nmsp'];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $meta->add([
            DF::quadNoSubject(DF::namedNode('https://some.property/1'), $skipNode),
            DF::quadNoSubject(DF::namedNode('https://some.property/2'), $addNode),
        ]);
        $resp  = $this->updateResource($meta);
        $this->assertEquals(200, $resp->getStatusCode());
        $m     = $this->extractResource($resp, $location);
        $g     = $m->getDataset();
        $this->assertFalse($g->any(new PT(self::$schema->id, object: $skipNode)));
        $this->assertFalse($m->any(new PT('https://some.property/1')));
        $this->assertCount(1, $g->copy(new PT(self::$schema->id, object: $addNode)));
        $added = $g->listSubjects(new PT(self::$schema->id, object: $addNode))->getValues();
        $this->assertEquals($added, $m->listObjects(new PT('https://some.property/2'))->getValues());

        // and deny
        $meta->add(DF::quadNoSubject(DF::namedNode('https://some.property/2'), DF::namedNode('https://deny.nmsp/123')));
        $resp = $this->updateResource($meta);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Denied to create a non-existing id', (string) $resp->getBody());
    }

    public function testWrongIds(): void {
        $txId    = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Type'                              => 'text/turtle',
            'Eppn'                                      => 'admin',
        ];

        $res  = new DatasetNode(self::$baseNode);
        $res->add(DF::quadNoSubject(self::$schema->id, DF::namedNode(self::$baseUrl . '0')));
        $req  = new Request('post', self::$baseUrl . 'metadata', $headers, self::$serializer->serialize($res));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertMatchesRegularExpression('/^Id in the repository base URL namespace which does not match the resource id/', (string) $resp->getBody());

        $res  = new DatasetNode(self::$baseNode);
        $res->add(DF::quadNoSubject(self::$schema->id, DF::literal(self::$baseUrl . '0')));
        $req  = new Request('post', self::$baseUrl . 'metadata', $headers, self::$serializer->serialize($res));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Non-resource identifier', (string) $resp->getBody());
    }

    function testVeryOldDate(): void {
        $meta     = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quadNoSubject(DF::namedNode('https://old/date1'), DF::literal('-12345-01-01', null, RDF::XSD_DATE)),
            DF::quadNoSubject(DF::namedNode('https://old/date2'), DF::literal('-4713-01-01', null, RDF::XSD_DATE)),
            DF::quadNoSubject(DF::namedNode('https://old/date3'), DF::literal('-4714-01-01', null, RDF::XSD_DATE)),
        ]);
        $location = $this->createMetadataResource($meta);
        $req      = new Request('get', $location . '/metadata');
        $resp     = self::$client->send($req);
        $g        = new DatasetNode(DF::namedNode($location));
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('-12345-01-01', $g->getObjectValue(new PT('https://old/date1')));
        $this->assertEquals('-4713-01-01', $g->getObjectValue(new PT('https://old/date2')));
        $this->assertEquals('-4714-01-01', $g->getObjectValue(new PT('https://old/date3')));
    }

    function testWrongValue(): void {
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quadNoSubject(DF::namedNode('https://wrong/date'), DF::literal('foo', null, RDF::XSD_DATE)));
        try {
            $this->createMetadataResource($meta);
            $this->assertTrue(false);
        } catch (RuntimeException $e) {
            $this->assertStringContainsString('Wrong property value', $e->getMessage());
        }
    }

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

        // geojson with BOM
        $headers['Content-Disposition'] = 'attachment; filename="test.geojson"';
        $headers['Content-Type']        = 'application/geo+json';
        $body                           = hex2bin('EFBBBF') . '{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}}, {"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 3]}}]}';
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
        $meta     = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quadNoSubject(self::$schema->id, DF::namedNode('https://' . rand())),
            DF::quadNoSubject(self::$schema->label, DF::literal('title')),
            DF::quadNoSubject(DF::namedNode(self::$config->spatialSearch->properties[0]), DF::literal('POLYGON((0 0,0 10,10 10,10 0,0 0))')),
        ]);
        $headers  = array_merge($this->getHeaders($txId), [
            'Content-Type' => 'application/n-triples'
        ]);
        $req      = new Request('post', self::$baseUrl . 'metadata', $headers, self::$serializer->serialize($meta));
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
            RRI::META_RELATIVES_ONLY, RRI::META_RELATIVES_REVERSE, RRI::META_RESOURCE,
            '0_0_0_0', '1', '1_2_1', '3_2_1_1', '-1_-3_0_0'];
        foreach ($readModes as $mode) {
            $req  = new Request('get', $location . '/metadata?readMode=' . rawurldecode($mode));
            $resp = self::$client->send($req);
            $this->assertEquals(200, $resp->getStatusCode());
        }

        $req  = new Request('get', $location . '/metadata?readMode=' . rawurldecode(RRI::META_NONE));
        $resp = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());

        $readModes = ['0_', 'foo', '0_0_0_foo', '0_0_0_2', '0_0_2_0', '1_2_-1'];
        foreach ($readModes as $mode) {
            $req  = new Request('get', $location . '/metadata?readMode=' . rawurldecode($mode));
            $resp = self::$client->send($req);
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString('Bad metadata mode', (string) $resp->getBody());
        }
    }

    public function testHttpRangeRequest(): void {
        $location = $this->createBinaryResource();
        $headers  = ['Eppn' => 'admin'];
        $resp     = self::$client->send(new Request('get', $location, $headers));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('bytes', $resp->getHeader('Accept-Ranges')[0] ?? '');
        $refData  = (string) $resp->getBody();
        $length   = strlen($refData);
        // In HTTP/1.1 and newer server may respond with transfer-encoding: chuncked which does not contain the content-length header
        if (count($resp->getHeader('Content-Length')) > 0) {
            $this->assertEquals(541, $resp->getHeader('Content-Length')[0]);
        }

        $chunkSize = 100;
        $data      = '';
        for ($i = 0; $i < $length; $i += $chunkSize) {
            $upperRange       = min($length - 1, $i + $chunkSize - 1);
            $headers['Range'] = "bytes=$i-$upperRange";
            $resp             = self::$client->send(new Request('get', $location, $headers));
            $this->assertEquals(206, $resp->getStatusCode());
            $chunk            = (string) $resp->getBody();
            $this->assertEquals($upperRange - $i + 1, strlen($chunk));
            if (count($resp->getHeader('Content-Length')) > 0) {
                $this->assertEquals('text/turtle;charset=UTF-8', $resp->getHeader('Content-Type')[0] ?? '');
            }
            $data .= $chunk;
        }
        $this->assertEquals($refData, $data);
    }

    public function testHttpMultiRangeRequest(): void {
        $location = $this->createBinaryResource();
        $headers  = ['Eppn' => 'admin'];
        $resp     = self::$client->send(new Request('get', $location, $headers));
        $data     = (string) $resp->getBody();

        $ranges           = [[50, 99], [75, 99], [25, 83]];
        $headers['Range'] = "bytes=" . implode(',', array_map(fn($x) => "$x[0]-$x[1]", $ranges));
        $resp             = self::$client->send(new Request('get', $location, $headers));
        $body             = (string) $resp->getBody();
        $this->assertEquals(206, $resp->getStatusCode());
        $this->assertStringStartsWith('multipart/byteranges; boundary=', $resp->getHeader('Content-Type')[0] ?? '');
        // In HTTP/1.1 and newer server may respond with transfer-encoding: chuncked which does not contain the content-length header
        if (count($resp->getHeader('Content-Length')) > 0) {
            $this->assertEquals(strlen($body), $resp->getHeader('Content-Length')[0] ?? -1);
        }
        $boundary = (string) preg_replace('/^.*boundary=/', '', trim($resp->getHeader('Content-Type')[0] ?? ''));
        $body     = explode("--$boundary", $body);
        $this->assertCount(count($ranges) + 2, $body);
        array_shift($body);
        for ($i = 0; $i < count($ranges); $i++) {
            $tmp = explode("\r\n", $body[$i]);
            $this->assertEquals('', $tmp[0]);
            $this->assertEquals('Content-Type: text/turtle', $tmp[1]);
            $this->assertEquals('Content-Range: bytes ' . $ranges[$i][0] . '-' . $ranges[$i][1] . '/' . strlen($data), $tmp[2]);
            $this->assertEquals('', $tmp[3]);
            $this->assertEquals(substr($data, $ranges[$i][0], $ranges[$i][1] - $ranges[$i][0] + 1), $tmp[4]);
        }
        $this->assertEquals("--\r\n", $body[$i]);
    }

    public function testHttpBadRangeRequest(): void {
        $location    = $this->createBinaryResource();
        $headers     = ['Eppn' => 'admin'];
        $wrongRanges = [
            'otherunit=0-10',
            'bytes=0-10000',
            'bytes=0-10,20-10000',
        ];
        foreach ($wrongRanges as $range) {
            $headers['Range'] = 'range';
            $resp             = self::$client->send(new Request('get', $location, $headers));
            $this->assertEquals(416, $resp->getStatusCode(), "range: $range");
        }
    }

    public function testETagLastModifiedDescibe(): void {
        $resp    = self::$client->send(new Request('get', self::$baseUrl . "describe"));
        $this->assertCount(1, $resp->getHeader('ETag'));
        $lastMod = date("D, d M Y H:i:s", filectime(__DIR__ . '/../config.yaml')) . " GMT";
        $this->assertEquals($lastMod, $resp->getHeader('Last-Modified')[0] ?? '');
        $this->assertEquals('no-cache', $resp->getHeader('Cache-Control')[0] ?? '');
    }

    public function testETagLastModifiedResource(): void {
        $txHeader = self::$config->rest->headers->transactionId;
        $headers  = [
            self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            'Eppn'                                         => 'admin',
        ];

        // create a resource and check if headers are there
        $location = $this->createBinaryResource();
        $resp     = self::$client->send(new Request('head', $location, $headers));
        $etag     = $resp->getHeader('Etag')[0] ?? '';
        $lastMod  = $resp->getHeader('Last-Modified')[0] ?? '';
        $this->assertNotEmpty($etag);
        $this->assertNotEmpty($lastMod);
        $this->assertEquals('no-cache', $resp->getHeader('Cache-Control')[0] ?? '');

        // change content and check both headers changed
        sleep(1);
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, 'test ETag nad Last-Modified headers');
        $resp               = self::$client->send($req);
        $this->commitTransaction($txId);
        unset($headers[$txHeader]);
        $resp               = self::$client->send(new Request('head', $location, $headers));
        $this->assertNotEquals($etag, $resp->getHeader('Etag')[0] ?? '');
        $this->assertNotEquals($lastMod, $resp->getHeader('Last-Modified')[0] ?? '');
        $etag               = $resp->getHeader('Etag')[0] ?? '';
        $lastMod            = $resp->getHeader('Last-Modified')[0] ?? '';

        // update the resource binary without changing the content - only Last-Modified should change
        sleep(1);
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, 'test ETag nad Last-Modified headers');
        $resp               = self::$client->send($req);
        $this->commitTransaction($txId);
        unset($headers[$txHeader]);
        $resp               = self::$client->send(new Request('head', $location, $headers));
        $this->assertEquals($etag, $resp->getHeader('ETag')[0] ?? '');
        $this->assertNotEquals($lastMod, $resp->getHeader('Last-Modified')[0] ?? '');
    }

    public function testFilterOutputProperties(): void {
        $fooBarProp = DF::namedNode('https://foo/bar');
        $bazBarProp = DF::namedNode('https://baz/bar');
        $fooBarTmpl = new PT($fooBarProp);
        $bazBarTmpl = new PT($bazBarProp);

        $meta     = new DatasetNode(self::$baseNode);
        $meta->add([
            DF::quadNoSubject($fooBarProp, DF::literal("baz")),
            DF::quadNoSubject($bazBarProp, DF::literal("foo")),
        ]);
        $location = $this->createMetadataResource($meta);

        $opts = ['query' => ['resourceProperties[0]' => $fooBarProp->getValue()]];
        $resp = self::$client->request('get', $location . '/metadata', $opts);
        $this->assertEquals(200, $resp->getStatusCode());
        $g    = new DatasetNode(DF::namedNode($location));
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals('baz', $g->getObjectValue($fooBarTmpl));
        $this->assertFalse($g->any($bazBarTmpl));

        $opts = ['headers' => [self::$config->rest->headers->resourceProperties => $bazBarProp->getValue()]];
        $resp = self::$client->request('get', $location . '/metadata', $opts);
        $this->assertEquals(200, $resp->getStatusCode());
        $g    = new DatasetNode(DF::namedNode($location));
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals('foo', $g->getObjectValue($bazBarTmpl));
        $this->assertFalse($g->any($fooBarTmpl));
    }

    public function testImageDimensions(): void {
        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Eppn'                                      => 'admin',
            'Content-Disposition'                       => 'attachment; filename="raster.tif"',
        ];

        // ordinary tif - shouldn't rise an error
        $body     = (string) file_get_contents(__DIR__ . '/data/raster.tif');
        $req      = new Request('post', self::$baseUrl, $headers, $body);
        $resp     = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0];
        $g        = new DatasetNode(DF::namedNode($location));
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals(366, $g->getObjectValue(new PT(self::$config->schema->imagePxHeight)));
        $this->assertEquals(668, $g->getObjectValue(new PT(self::$config->schema->imagePxWidth)));

        // geoTIFF
        $body     = (string) file_get_contents(__DIR__ . '/data/georaster.tif');
        $resp     = self::$client->send(new Request('post', self::$baseUrl, $headers, $body));
        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0];
        $g        = new DatasetNode(DF::namedNode($location));
        $g->add(RdfIoUtil::parse($resp, new DF()));
        $this->assertEquals(366, $g->getObjectValue(new PT(self::$config->schema->imagePxHeight)));
        $this->assertEquals(668, $g->getObjectValue(new PT(self::$config->schema->imagePxWidth)));

        $this->rollbackTransaction($txId);
    }

    /**
     * Tests hardcore failures on binary upload which should not normally happen.
     * Still we want to make sure they are handled correctly and no binaries get
     * corrupted, etc.
     */
    public function testPutErrors1(): void {
        // create meta-only resource
        $location   = $this->createMetadataResource();
        $id         = (int) preg_replace('`^.*/`', '', $location);
        $binaryPath = BinaryPayload::getStorageDir($id, self::$config->storage->dir, 0, self::$config->storage->levels) . '/' . $id;
        $req        = new Request('get', $location, $this->getHeaders());
        $resp       = self::$client->send($req);
        $this->assertEquals(302, $resp->getStatusCode());
        $this->assertFileDoesNotExist($binaryPath);

        $tmpDir   = self::$config->storage->tmpDir;
        $txHeader = self::$config->rest->headers->transactionId;
        $headers  = [
            self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            'Content-Disposition'                          => 'attachment; filename="RestTest.php"',
            'Content-Type'                                 => 'application/php',
            'Eppn'                                         => 'admin',
        ];
        $body     = (string) file_get_contents(__FILE__);

        // scenario 1. breaking the full text search indexing throws error in BinaryPayload::upload()
        self::$pdo->beginTransaction();
        self::$pdo->query("LOCK TABLE full_text_search IN EXCLUSIVE MODE");
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, $body);
        $resp               = self::$client->send($req);
        self::$pdo->rollBack();
        $this->assertEquals(409, $resp->getStatusCode());
        $this->waitForTransactionEnd($txId);
        $this->assertCount(2, scandir($tmpDir));
        $this->assertCount(2, scandir(dirname($binaryPath)));

        // scenario 2. breaking the final metadata save after handlers 
        // throws error almost at the end of the Resource::put()
        self::$pdo->beginTransaction();
        self::$pdo->query("LOCK TABLE metadata IN EXCLUSIVE MODE");
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, $body);
        $resp               = self::$client->send($req);
        self::$pdo->rollBack();
        $this->assertEquals(409, $resp->getStatusCode());
        $this->waitForTransactionEnd($txId);
        $this->assertCount(2, scandir($tmpDir));
        $this->assertCount(2, scandir(dirname($binaryPath)));
    }

    /**
     * Tests hardcore failures on binary upload which should not normally happen.
     * Still we want to make sure they are handled correctly and no binaries get
     * corrupted, etc.
     */
    public function testPutErrors2(): void {
        // create meta-only resource
        $location   = $this->createBinaryResource();
        $id         = (int) preg_replace('`^.*/`', '', $location);
        $binaryPath = BinaryPayload::getStorageDir($id, self::$config->storage->dir, 0, self::$config->storage->levels) . '/' . $id;
        $req        = new Request('get', $location, $this->getHeaders());
        $resp       = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertFileExists($binaryPath);
        $refContent = file_get_contents($binaryPath);

        $tmpDir   = self::$config->storage->tmpDir;
        $txHeader = self::$config->rest->headers->transactionId;
        $headers  = [
            self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            'Content-Disposition'                          => 'attachment; filename="RestTest.php"',
            'Content-Type'                                 => 'application/php',
            'Eppn'                                         => 'admin',
        ];
        $body     = (string) file_get_contents(__FILE__);

        // scenario 1. breaking the full text search indexing throws error in BinaryPayload::upload()
        self::$pdo->beginTransaction();
        self::$pdo->query("LOCK TABLE full_text_search IN EXCLUSIVE MODE");
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, $body);
        $resp               = self::$client->send($req);
        self::$pdo->rollBack();
        $this->assertEquals(409, $resp->getStatusCode());
        $this->waitForTransactionEnd($txId);
        $this->assertCount(2, scandir($tmpDir));
        $this->assertCount(3, scandir(dirname($binaryPath)));
        $this->assertEquals($refContent, file_get_contents($binaryPath));

        // scenario 2. breaking the final metadata save after handlers 
        // throws error almost at the end of the Resource::put()
        self::$pdo->beginTransaction();
        self::$pdo->query("LOCK TABLE metadata IN EXCLUSIVE MODE");
        $txId               = $this->beginTransaction();
        $headers[$txHeader] = $txId;
        $req                = new Request('put', $location, $headers, $body);
        $resp               = self::$client->send($req);
        self::$pdo->rollBack();
        $this->assertEquals(409, $resp->getStatusCode());
        $this->waitForTransactionEnd($txId);
        $this->assertCount(2, scandir($tmpDir));
        $this->assertCount(3, scandir(dirname($binaryPath)));
        $this->assertEquals($refContent, file_get_contents($binaryPath));
    }
}
