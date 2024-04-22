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
use PHPUnit\Framework\Attributes\Group;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use zozlak\auth\usersDb\PdoDb;
use zozlak\auth\authMethod\HttpBasic;

/**
 * Description of AuthTest
 *
 * @author zozlak
 */
class AuthTest extends TestBase {

    private string $cfgBak;

    public function setUp(): void {
        parent::setUp();
        $this->cfgBak = file_get_contents(__DIR__ . '/../config.yaml');
    }

    public function tearDown(): void {
        parent::tearDown();
        file_put_contents(__DIR__ . '/../config.yaml', $this->cfgBak);
    }

    /**
     * 
     * #[Group('auth')]
     */
    public function testHeader(): void {
        $location = $this->createBinaryResource();

        $cfg                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['accessControl']['authMethods'] = [
            [
                'class'      => '\zozlak\auth\authMethod\TrustedHeader',
                'parameters' => ['HTTP_FOO'],
            ]
        ];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $txId    = $this->beginTransaction();
        $headers = [self::$config->rest->headers->transactionId => (string) $txId];

        $req  = new Request('delete', $location, $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        $req  = new Request('delete', $location, array_merge($headers, ['foo' => 'badUser']));
        $resp = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        $req  = new Request('delete', $location, array_merge($headers, ['foo' => 'admin']));
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $this->rollbackTransaction($txId);
    }

    /**
     * 
     * #[Group('auth')]
     */
    public function testHttpBasic(): void {
        $cfg                                 = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['accessControl']['authMethods'] = [
            [
                'class'      => '\zozlak\auth\authMethod\HttpBasic',
                'parameters' => ['repo'],
            ]
        ];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $cfg  = self::$config->accessControl;
        $db   = new PdoDb($cfg->db->connStr, $cfg->db->table, $cfg->db->userCol, $cfg->db->dataCol);
        $user = $cfg->create->allowedRoles[0];
        $pswd = '123qwe';
        $db->putUser($user, HttpBasic::pswdData($pswd));

        $txId    = $this->beginTransaction();
        $headers = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Disposition'                       => 'attachment; filename="test.ttl"',
            'Content-Type'                              => 'text/turtle',
        ];
        $body    = (string) file_get_contents(__DIR__ . '/data/test.ttl');

        $req  = new Request('post', self::$baseUrl, $headers, $body);
        $resp = self::$client->send($req);
        $this->assertEquals(401, $resp->getStatusCode());
        $this->assertEquals(['Basic realm="repo"'], $resp->getHeader('WWW-Authenticate'));

        $headers['Authorization'] = 'Basic ' . base64_encode("$user:_wrong_password_");
        $req                      = new Request('post', self::$baseUrl, $headers, $body);
        $resp                     = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        $headers['Authorization'] = 'Basic ' . base64_encode("$user:$pswd");
        $req                      = new Request('post', self::$baseUrl, $headers, $body);
        $resp                     = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
    }

    /**
     * 
     * #[Group('auth')]
     */
    public function testEnforceOnMeta(): void {
        $location = $this->createBinaryResource();
        $req      = new Request('get', $location . '/metadata');

        $cfg                                                   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['accessControl']['create']['assignRoles']['read'] = [];
        $cfg['accessControl']['enforceOnMetadata']             = false;
        $cfg['accessControl']['defaultAction']['read']         = 'deny';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        $resp                                                  = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());

        $cfg                                                   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['accessControl']['create']['assignRoles']['read'] = [];
        $cfg['accessControl']['enforceOnMetadata']             = true;
        $cfg['accessControl']['defaultAction']['read']         = 'deny';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        $resp                                                  = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
    }

    /**
     * 
     * #[Group('auth')]
     */
    public function testAssignOnCreate(): void {
        $cfg                                                   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['accessControl']['create']['assignRoles']['read'] = ['publicRole'];
        $cfg['accessControl']['enforceOnMetadata']             = true;
        $cfg['accessControl']['defaultAction']['read']         = 'deny';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $location = $this->createBinaryResource();
        $req      = new Request('get', $location . '/metadata');
        $resp     = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * 
     * #[Group('auth')]
     */
    public function testAuthDeleteRecursively(): void {
        $cfg     = yaml_parse_file(__DIR__ . '/../config.yaml');
        $relProp = 'http://relation';
        $txId    = $this->beginTransaction();

        $cfg['accessControl']['create']['assignRoles']['write'] = ['publicRole'];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        $loc1                                                   = $this->createMetadataResource(null, $txId);

        $cfg['accessControl']['create']['assignRoles']['write'] = [];
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        $meta                                                   = new DatasetNode(self::$baseNode);
        $meta->add(DF::quad(self::$baseNode, DF::namedNode($relProp), DF::namedNode($loc1)));
        $loc2                                                   = $this->createMetadataResource($meta, $txId);

        $headers = [
            self::$config->rest->headers->transactionId          => (string) $txId,
            self::$config->rest->headers->metadataParentProperty => $relProp,
        ];
        $req     = new Request('delete', $loc1, $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        $headers = [
            self::$config->rest->headers->transactionId  => (string) $txId,
            self::$config->rest->headers->withReferences => 1,
        ];
        $req     = new Request('delete', $loc1, $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
    }
}
