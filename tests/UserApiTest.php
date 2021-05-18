<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
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
use zozlak\auth\usersDb\PdoDb;
use zozlak\auth\authMethod\HttpBasic;
use function \GuzzleHttp\json_encode;

/**
 * Description of UserApiTest
 *
 * @author zozlak
 */
class UserApiTest extends TestBase {

    const PSWD = 'baz';

    static private string $admin;
    static private string $createGroup;
    static private string $adminAuth;
    static private string $publicGroup;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        self::$pdo->query('DELETE FROM users');

        $cfg         = self::$config->accessControl;
        $db          = new PdoDb($cfg->db->connStr, $cfg->db->table, $cfg->db->userCol, $cfg->db->dataCol);
        self::$admin = $cfg->adminRoles[0];
        $db->putUser(self::$admin, HttpBasic::pswdData('strongPassword'));

        self::$createGroup = $cfg->create->allowedRoles[0];
        self::$publicGroup = $cfg->publicRole;
        self::$adminAuth   = 'Basic ' . base64_encode(self::$admin . ':strongPassword');
    }

    static public function tearDownAfterClass(): void {
        self::$pdo->query('DELETE FROM users');

        parent::tearDownAfterClass();
    }

    /**
     * 
     * @group userApi
     */
    public function testUserCreate(): void {
        // QUERY
        $headers = ['Authorization' => self::$adminAuth];
        $query   = '?groups[]=' . urlencode(self::$createGroup) . '&password=' . self::PSWD;
        $req     = new Request('put', self::$baseUrl . 'user/foo' . $query, $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $data    = json_decode($resp->getBody());
        $this->assertEquals('foo', $data->userId);
        $this->assertEquals(2, count($data->groups));
        $this->assertContains(self::$createGroup, $data->groups);
        $this->assertContains(self::$publicGroup, $data->groups);

        // X-WWW-URLENCODED and non-array groups
        $headers['Content-Type'] = 'application/x-www-form-urlencoded';
        $body                    = 'password=' . self::PSWD;
        $req                     = new Request('put', self::$baseUrl . 'user/bar', $headers, $body);
        $resp                    = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $data                    = json_decode($resp->getBody());
        $this->assertEquals('bar', $data->userId);
        $this->assertEquals(1, count($data->groups));
        $this->assertContains(self::$publicGroup, $data->groups);

        // JSON
        $headers['Content-Type'] = 'application/json';
        $body                    = json_encode([
            'groups'   => [],
            'password' => self::PSWD
        ]);
        $req                     = new Request('put', self::$baseUrl . 'user/baz', $headers, $body);
        $resp                    = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $data                    = json_decode($resp->getBody());
        $this->assertEquals('baz', $data->userId);
        $this->assertEquals(1, count($data->groups));
        $this->assertContains(self::$publicGroup, $data->groups);

        // lack of priviledges
        $headers['Authorization'] = 'Basic ' . base64_encode('foo:' . self::PSWD);
        $req                      = new Request('put', self::$baseUrl . 'user/foobar', $headers, $body);
        $resp                     = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
    }

    /**
     * @depends testUserCreate
     * @group userApi
     */
    public function testUserGet(): void {
        // as root
        $headers = ['Authorization' => self::$adminAuth];
        $req     = new Request('get', self::$baseUrl . 'user', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data    = json_decode($resp->getBody());
        $this->assertCount(4, $data);
        foreach ($data as $i) {
            $this->assertContains(self::$publicGroup, $i->groups);
        }

        $data = json_decode($resp->getBody());
        $req  = new Request('get', self::$baseUrl . 'user/foo', $headers);
        $resp = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data = json_decode($resp->getBody());
        $this->assertEquals('foo', $data->userId);
        $this->assertEquals(2, count($data->groups));
        $this->assertContains(self::$createGroup, $data->groups);
        $this->assertContains(self::$publicGroup, $data->groups);
        $this->assertFalse(isset($data->password));
        $this->assertFalse(isset($data->pswd));

        // as user
        $headers = ['Authorization' => 'Basic ' . base64_encode('bar:' . self::PSWD)];
        $req     = new Request('get', self::$baseUrl . 'user/bar', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data    = json_decode($resp->getBody());
        $this->assertEquals('bar', $data->userId);
        $this->assertEquals(1, count($data->groups));
        $this->assertContains(self::$publicGroup, $data->groups);
        $this->assertFalse(isset($data->password));
        $this->assertFalse(isset($data->pswd));

        // lack of priviledges
        $headers = ['Authorization' => 'Basic ' . base64_encode('foo:' . self::PSWD)];
        $req     = new Request('get', self::$baseUrl . 'user/bar', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
        $req     = new Request('get', self::$baseUrl . 'user/', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        // wrong password
        $headers = ['Authorization' => 'Basic ' . base64_encode('bar:xxx')];
        $req     = new Request('get', self::$baseUrl . 'user/bar', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        // non-existing user
        $req     = new Request('get', self::$baseUrl . 'user/joe', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
        $headers = ['Authorization' => self::$adminAuth];
        $req     = new Request('get', self::$baseUrl . 'user/joe', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(404, $resp->getStatusCode());
    }

    /**
     * @depends testUserCreate
     * @group userApi
     */
    public function testUserPatch(): void {
        // as root
        $headers        = ['Authorization' => self::$adminAuth, 'Content-Type' => 'application/json'];
        $body           = json_encode([
            'groups'   => [self::$createGroup, 'foobar'],
            'other'    => 'value',
            'password' => 'newPass',
        ]);
        $req            = new Request('patch', self::$baseUrl . 'user/foo', $headers, $body);
        $resp           = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data           = json_decode($resp->getBody());
        $this->assertEquals('foo', $data->userId);
        $this->assertEquals(3, count($data->groups));
        $expectedGroups = [self::$createGroup, self::$publicGroup, 'foobar'];
        $this->assertEquals(3, count(array_intersect($expectedGroups, $data->groups)));
        $this->assertFalse(isset($data->password));
        $this->assertFalse(isset($data->pswd));
        $this->assertFalse(isset($data->other));

        // as user
        $headers = ['Authorization' => 'Basic ' . base64_encode('foo:' . self::PSWD)];
        $body    = json_encode([
            'groups'   => [],
            'other'    => 'value',
            'password' => self::PSWD,
        ]);
        $req     = new Request('patch', self::$baseUrl . 'user/foo', $headers, $body);
        $resp    = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
        $headers = ['Authorization' => 'Basic ' . base64_encode('foo:newPass')];
        $req     = new Request('patch', self::$baseUrl . 'user/foo', $headers, $body);
        $resp    = self::$client->send($req);
        $this->assertEquals(200, $resp->getStatusCode());
        $data    = json_decode($resp->getBody());
        $this->assertEquals('foo', $data->userId);
        $this->assertEquals([self::$publicGroup], $data->groups);
        $this->assertFalse(isset($data->password));
        $this->assertFalse(isset($data->pswd));
        $this->assertFalse(isset($data->other));
    }

    /**
     * 
     * @group userApi
     */
    public function testUserDelete(): void {
        // as user
        $headers = ['Authorization' => 'Basic ' . base64_encode('foo:' . self::PSWD)];
        $req     = new Request('delete', self::$baseUrl . 'user/foo', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $resp    = self::$client->send($req->withMethod('get'));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp    = self::$client->send($req->withMethod('get')->withHeader('Authorization', self::$adminAuth));
        $this->assertEquals(404, $resp->getStatusCode());

        // as admin
        $headers = ['Authorization' => self::$adminAuth];
        $req     = new Request('delete', self::$baseUrl . 'user/bar', $headers);
        $resp    = self::$client->send($req);
        $this->assertEquals(204, $resp->getStatusCode());
        $resp    = self::$client->send($req->withMethod('get'));
        $this->assertEquals(404, $resp->getStatusCode());
    }
}
