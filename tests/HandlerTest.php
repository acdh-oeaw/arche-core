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
use quickRdf\DataFactory as DF;
use quickRdf\Dataset;
use zozlak\RdfConstants as RDF;
use termTemplates\QuadTemplate as QT;
use termTemplates\NamedNodeTemplate;
use termTemplates\LiteralTemplate;
use acdhOeaw\arche\core\HandlersController as HC;

/**
 * Description of HandlerTest
 *
 * @author zozlak
 */
class HandlerTest extends TestBase {

    private mixed $rmqSrvr;

    public function setUp(): void {
        parent::setUp();

        // clear all handlers
        $cfg = yaml_parse_file(__DIR__ . '/../config.yaml');
        foreach ($cfg['rest']['handlers']['methods'] as &$i) {
            $i = [];
        };
        unset($i);
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
    }

    public function tearDown(): void {
        parent::tearDown();

        if (!empty($this->rmqSrvr)) {
            // proc_open() runs the command by invoking shell, so the actual process's PID is (if everything goes fine) one greater
            $s             = proc_get_status($this->rmqSrvr);
            posix_kill($s['pid'] + 1, 15);
            proc_close($this->rmqSrvr);
            $this->rmqSrvr = null;
        }
    }

    /**
     * @group handler
     */
    public function testNoHandlers(): void {
        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertFalse($meta->any(new QT(predicate: DF::namedNode('https://text'))));
        $this->assertFalse($meta->any(new QT(predicate: DF::namedNode('https://default'))));
    }

    /**
     * @group handler
     */
    public function testWrongHandler(): void {
        $this->setHandlers([
            'create'   => ['type' => 'foo'],
            'txCommit' => ['type' => 'bar'],
        ]);
        $txId = $this->beginTransaction();
        $this->assertGreaterThan(0, $txId);

        $req  = new Request('post', self::$baseUrl, $this->getHeaders($txId), 'foo bar');
        $resp = self::$client->send($req);
        $this->assertEquals(500, $resp->getStatusCode());
        $this->assertEquals('Unknown handler type: foo', (string) $resp->getBody());

        $req  = new Request('put', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(500, $resp->getStatusCode());
        $this->assertEquals('Unknown handler type: bar', (string) $resp->getBody());
    }

    /**
     * 
     * @group handler
     */
    public function testMetadataManagerBasic(): void {
        $this->setHandlers([
            'create' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertTrue($meta->listObjects(new QT(predicate: DF::namedNode('https://text')))->current()?->equals(DF::literal('sample text', 'en')));
        $this->assertTrue($meta->listObjects(new QT(predicate: DF::namedNode('https://other')))->current()->equals(DF::literal('own type', null, 'https://own/type')));
        $this->assertEquals('https://rdf/type', $this->extractValue($meta, RDF::RDF_TYPE));
        $this->assertEquals('sample value', $this->extractValue($meta, 'https://default'));
    }

    /**
     * @group handler
     */
    public function testMetadataManagerDefault(): void {
        $defaultProp = DF::namedNode('https://default');
        $defaultTmpl = new QT(predicate: $defaultProp);
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $this->updateResource($this->getResourceMeta($location));

        $meta1 = $this->getResourceMeta($location);
        $this->assertEquals('sample value', $meta1->listObjects($defaultTmpl)->current()?->getValue());

        $meta1->delete($defaultTmpl);
        $meta1->add(DF::quad($meta1->getNode(), $defaultProp, DF::literal('other value')));
        $this->updateResource($meta1);

        $meta2 = $this->getResourceMeta($location);
        $this->assertCount(1, $meta2->copy($defaultTmpl));
        $this->assertEquals('other value', $meta2->listObjects($defaultTmpl)->current()?->getValue());
    }

    /**
     * @group handler
     */
    public function testMetadataManagerForbidden(): void {
        $predicate = DF::namedNode('https://forbidden');
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $meta->add([
            DF::quad($meta->getNode(), $predicate, DF::literal('test', 'en')),
            DF::quad($meta->getNode(), $predicate, DF::namedNode('https://whatever')),
        ]);
        $this->updateResource($meta);

        $newMeta = $this->getResourceMeta($location);
        $this->assertCount(0, $newMeta->copy(new QT(predicate: $predicate)));
    }

    /**
     * @group handler
     */
    public function testMetadataManagerCopying(): void {
        $fromProp = DF::namedNode('https://copy/from');
        $tmpl     = new QT(predicate: DF::namedNode('https://copy/to'), object: new LiteralTemplate(null, LiteralTemplate::ANY));

        $this->setHandlers([
            'updateMetadata' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $meta->add([
            DF::quad($meta->getNode(), $fromProp, DF::literal('test', 'en')),
            DF::quad($meta->getNode(), $fromProp, DF::namedNode('https://whatever')),
        ]);
        $this->updateResource($meta);

        $newMeta = $this->getResourceMeta($location);
        $this->assertTrue($newMeta->listObjects($tmpl)->current()?->equals(DF::literal('test', 'en')));
    }

    /**
     * @group handler
     */
    public function testRpcBasic(): void {
        $this->setHandlers([
            'create' => [
                'type'  => HC::TYPE_RPC,
                'queue' => 'onCreateRpc',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertEquals('create rpc', $this->extractValue($meta, 'https://rpc/property'));
    }

    /**
     * @group handler
     */
    public function testRpcError(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'  => HC::TYPE_RPC,
                'queue' => 'onCreateRpcError',
            ],
            ], true);

        $location = $this->createBinaryResource();
        $this->getResourceMeta($location);

        $resp = $this->updateResource($this->getResourceMeta($location));
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertStringContainsString('metadata is always wrong', (string) $resp->getBody());
    }

    /**
     * @group handler
     */
    public function testRpcTimeoutException(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'  => HC::TYPE_RPC,
                'queue' => 'onUpdateRpc',
            ],
            ], true);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertTrue($meta->none(new QT(predicate: DF::namedNode('https://rpc/property'))));

        $resp = $this->updateResource($this->getResourceMeta($location));
        $this->assertEquals(500, $resp->getStatusCode());
    }

    /**
     * @group handler
     */
    public function testRpcTimeoutNoException(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'  => HC::TYPE_RPC,
                'queue' => 'onUpdateRpc',
            ],
            ], false);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertTrue($meta->none(new QT(predicate: DF::namedNode('https://rpc/property'))));

        $resp = $this->updateResource($this->getResourceMeta($location));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * @group handler
     */
    public function testTxCommitFunction(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\tests\Handler::onTxCommit',
            ],
        ]);

        $txId      = $this->beginTransaction();
        $location1 = $this->createBinaryResource($txId);
        $location2 = $this->createBinaryResource($txId);
        $this->commitTransaction($txId);
        $meta1     = $this->getResourceMeta($location1);
        $meta2     = $this->getResourceMeta($location2);
        $tmpl      = new QT(predicate: DF::namedNode('https://commit/property'));
        $this->assertEquals('commit' . $txId, $meta1->listObjects($tmpl)->current()?->getValue());
        $this->assertEquals('commit' . $txId, $meta2->listObjects($tmpl)->current()?->getValue());
    }

    /**
     * @group handler
     */
    public function testTxCommitRpc(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'  => HC::TYPE_RPC,
                'queue' => 'onCommitRpc',
            ],
        ]);

        $txId      = $this->beginTransaction();
        $location1 = $this->createBinaryResource($txId);
        $location2 = $this->createBinaryResource($txId);
        $this->commitTransaction($txId);
        $meta1     = $this->getResourceMeta($location1);
        $meta2     = $this->getResourceMeta($location2);
        $tmpl      = new QT(predicate: DF::namedNode('https://commit/property'));
        $this->assertEquals('commit' . $txId, $meta1->listObjects($tmpl)->current()?->getValue());
        $this->assertEquals('commit' . $txId, $meta2->listObjects($tmpl)->current()?->getValue());
    }

    /**
     * @group handler
     */
    public function testFunctionHandler(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'     => HC::TYPE_FUNC,
                'function' => 'max',
            ]
        ]);
        $txId = $this->beginTransaction();
        $this->assertEquals(204, $this->commitTransaction($txId));
    }

    /**
     * Tests if a on-metadata-edit handler can prevent deletion with references removal
     * @group handler
     */
    public function testDeleteWithReferencesHandler(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\tests\Handler::deleteReference',
            ],
        ]);

        $txId                                                  = $this->beginTransaction();
        $headers                                               = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->withReferences] = 1;

        $loc1 = $this->createMetadataResource(null, $txId);        
        $meta = new Dataset();
        $meta->add([
            DF::quad(self::$baseNode, DF::namedNode(Handler::CHECKTRIGGER_PROP), DF::literal('baz')),
            DF::quad(self::$baseNode, DF::namedNode(Handler::CHECK_PROP), DF::namedNode($loc1)),
        ]);
        $this->createMetadataResource($meta, $txId);

        $req  = new Request('delete', $loc1, $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals(204, $this->commitTransaction($txId));
        $this->assertStringContainsString(Handler::CHECK_PROP . ' is missing', (string) $resp->getBody());
    }

    /**
     * Tests if resource creation refused by the post-creation handler doesn't
     * leave any trash in the database.
     * 
     * @group handler
     */
    public function testRefusedCreation(): void {
        $this->setHandlers([
            'create' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\tests\Handler::throwException',
            ]
        ]);
        $cfg = yaml_parse_file(__DIR__ . '/../config.yaml');
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $txId = $this->beginTransaction();
        try {
            $this->createBinaryResource($txId);
            $this->assertTrue(false);
        } catch (RuntimeException $ex) {
            $this->assertEquals(400, $ex->getCode());
            $this->assertEquals("Just throw an exception", $ex->getMessage());
        }
        try {
            $this->createMetadataResource(null, $txId);
            $this->assertTrue(false);
        } catch (RuntimeException $ex) {
            $this->assertEquals(400, $ex->getCode());
            $this->assertEquals("Just throw an exception", $ex->getMessage());
        }
        $count = self::$pdo->query("SELECT count(*) FROM resources")->fetchColumn();
        $this->assertEquals(0, $count);
    }

    /**
     * @group handler
     * 
     * REMARK - HAS TO BE THE SECOND LAST TEST IN THIS CLASS AS IT BREAKS THE CONFIG
     */
    public function testBrokenHandler(): void {
        $this->setHandlers([
            'create' => [
                'type'     => HC::TYPE_FUNC,
                'function' => '\acdhOeaw\arche\core\tests\Handler::brokenHandler',
            ]
        ]);
        $cfg                                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['transactionController']['enforceCompleteness'] = true;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $txId = $this->beginTransaction();
        try {
            $this->createBinaryResource($txId);
            $this->assertTrue(false);
        } catch (RuntimeException $ex) {
            $this->assertEquals(500, $ex->getCode());
            $this->assertEmpty($ex->getMessage());
        }

        $req  = new Request('get', self::$baseUrl . 'transaction', $this->getHeaders($txId));
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals("Transaction $txId doesn't exist", (string) $resp->getBody());
    }

    /**
     * Tests if server-side initialization error are captured correctly and no
     * information about the error is leaked.
     * 
     * REMARK - HAS TO BE THE LAST TEST IN THIS CLASS AS IT BREAKS THE CONFIG
     * 
     * @group handler
     */
    public function testWrongSetup(): void {
        $cfg                                         = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['rest']['handlers']['rabbitMq']['host'] = 'foo';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $req  = new Request('post', self::$baseUrl . 'transaction');
        $resp = self::$client->send($req);
        $this->assertEquals(500, $resp->getStatusCode());
        $this->assertEquals('Internal Server Error', $resp->getReasonPhrase());
        $this->assertEmpty((string) $resp->getBody());
    }

    /**
     * 
     * @param array<string, array<string, string>> $handlers
     * @param bool $exOnRpcTimeout
     * @return void
     * @throws RuntimeException
     */
    private function setHandlers(array $handlers, bool $exOnRpcTimeout = false): void {
        $cfg = yaml_parse_file(__DIR__ . '/../config.yaml');
        foreach ($handlers as $method => $data) {
            $cfg['rest']['handlers']['methods'][$method][] = $data;
        }
        $cfg['rest']['handlers']['rabbitMq']['exceptionOnTimeout'] = $exOnRpcTimeout;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $cmd           = 'php -f ' . __DIR__ . '/handlerRun.php ' . __DIR__ . '/../config.yaml';
        $desc          = [2 => ['pipe', 'w']]; // catch stdout to avoid the "Terminated" output
        $pipes         = [];
        $this->rmqSrvr = proc_open($cmd, $desc, $pipes, __DIR__);
        if ($this->rmqSrvr === false) {
            throw new RuntimeException('failed to start handlerRun.php');
        }
    }
}
