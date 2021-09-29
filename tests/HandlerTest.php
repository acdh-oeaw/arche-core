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
use GuzzleHttp\Psr7\Request;

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
        $this->assertNull($meta->getLiteral('https://text'));
        $this->assertNull($meta->getLiteral('https://default'));
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
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertEquals('sample text', (string) $meta->getLiteral('https://text'));
        $this->assertEquals('en', $meta->getLiteral('https://text')?->getLang());
        $this->assertEquals('own type', (string) $meta->getLiteral('https://other'));
        $this->assertEquals('https://own/type', $meta->getLiteral('https://other')?->getDatatypeUri());
        $this->assertEquals('https://rdf/type', (string) $meta->getResource('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'));
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://default'));
    }

    /**
     * @group handler
     */
    public function testMetadataManagerDefault(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $this->updateResource($this->getResourceMeta($location));

        $meta1 = $this->getResourceMeta($location);
        $this->assertEquals('sample value', (string) $meta1->get('https://default'));

        $meta1->delete('https://default');
        $meta1->addLiteral('https://default', 'other value');
        $this->updateResource($meta1);

        $meta2 = $this->getResourceMeta($location);
        $this->assertEquals(1, count($meta2->all('https://default')));
        $this->assertEquals('other value', (string) $meta2->get('https://default'));
    }

    /**
     * @group handler
     */
    public function testMetadataManagerForbidden(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $meta->addLiteral('https://forbidden', 'test', 'en');
        $meta->addResource('https://forbidden', 'https://whatever');
        $this->updateResource($meta);

        $newMeta = $this->getResourceMeta($location);
        $this->assertEquals(0, count($newMeta->all('https://forbidden')));
    }

    /**
     * @group handler
     */
    public function testMetadataManagerCopying(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\handler\MetadataManager::manage',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $meta->addLiteral('https://copy/from', 'test', 'en');
        $meta->addResource('https://copy/from', 'https://whatever');
        $this->updateResource($meta);

        $newMeta = $this->getResourceMeta($location);
        $this->assertEquals('test', (string) $newMeta->getLiteral('https://copy/to'));
        $this->assertEquals('en', $newMeta->getLiteral('https://copy/to')?->getLang());
    }

    /**
     * @group handler
     */
    public function testRpcBasic(): void {
        $this->setHandlers([
            'create' => [
                'type'  => 'rpc',
                'queue' => 'onCreateRpc',
            ],
        ]);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertEquals('create rpc', (string) $meta->get('https://rpc/property'));
    }

    /**
     * @group handler
     */
    public function testRpcTimeoutExceptio(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'  => 'rpc',
                'queue' => 'onUpdateRpc',
            ],
            ], true);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertNull($meta->get('https://rpc/property'));

        $resp = $this->updateResource($this->getResourceMeta($location));
        $this->assertEquals(500, $resp->getStatusCode());
    }

    public function testRpcTimeoutNoException(): void {
        $this->setHandlers([
            'updateMetadata' => [
                'type'  => 'rpc',
                'queue' => 'onUpdateRpc',
            ],
            ], false);

        $location = $this->createBinaryResource();
        $meta     = $this->getResourceMeta($location);
        $this->assertNull($meta->get('https://rpc/property'));

        $resp = $this->updateResource($this->getResourceMeta($location));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * @group handler
     */
    public function testTxCommitFunction(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\tests\Handler::onTxCommit',
            ],
        ]);

        $txId      = $this->beginTransaction();
        $location1 = $this->createBinaryResource($txId);
        $location2 = $this->createBinaryResource($txId);
        $this->commitTransaction($txId);
        $meta1     = $this->getResourceMeta($location1);
        $meta2     = $this->getResourceMeta($location2);
        $this->assertEquals('commit' . $txId, (string) $meta1->getLiteral('https://commit/property'));
        $this->assertEquals('commit' . $txId, (string) $meta2->getLiteral('https://commit/property'));
    }

    /**
     * @group handler
     */
    public function testTxCommitRpc(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'  => 'rpc',
                'queue' => 'onCommitRpc',
            ],
        ]);

        $txId      = $this->beginTransaction();
        $location1 = $this->createBinaryResource($txId);
        $location2 = $this->createBinaryResource($txId);
        $this->commitTransaction($txId);
        $meta1     = $this->getResourceMeta($location1);
        $meta2     = $this->getResourceMeta($location2);
        $this->assertEquals('commit' . $txId, (string) $meta1->getLiteral('https://commit/property'));
        $this->assertEquals('commit' . $txId, (string) $meta2->getLiteral('https://commit/property'));
    }

    /**
     * @group handler
     */
    public function testFunctionHandler(): void {
        $this->setHandlers([
            'txCommit' => [
                'type'     => 'function',
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
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\tests\Handler::deleteReference',
            ],
        ]);

        $txId                                                  = $this->beginTransaction();
        $headers                                               = $this->getHeaders($txId);
        $headers[self::$config->rest->headers->withReferences] = 1;

        $loc1 = $this->createMetadataResource(null, $txId);
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addLiteral(Handler::CHECKTRIGGER_PROP, "baz");
        $meta->addResource(Handler::CHECK_PROP, $loc1);
        $this->createMetadataResource($meta, $txId);

        $req  = new Request('delete', $loc1, $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals(204, $this->commitTransaction($txId));
        $this->assertStringContainsString(Handler::CHECK_PROP . ' is missing', (string) $resp->getBody());
    }        
    
    /**
     * @group handler
     * 
     * REMARK - HAS TO BE THE SECOND LAST TEST IN THIS CLASS AS IT BREAKS THE CONFIG
     */
    public function testBrokenHandler(): void {
        $this->setHandlers([
            'create' => [
                'type'     => 'function',
                'function' => '\acdhOeaw\arche\core\tests\Handler::brokenHandler',
            ]
        ]);
        $cfg                                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['transactionController']['enforceCompleteness'] = true;
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        self::reloadTxCtrlConfig();

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
        $this->assertEquals('Unknown transaction', (string) $resp->getBody());
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
        $pipes         = [];
        $this->rmqSrvr = proc_open($cmd, [], $pipes, __DIR__);
        if ($this->rmqSrvr === false) {
            throw new RuntimeException('failed to start handlerRun.php');
        }
    }
}
