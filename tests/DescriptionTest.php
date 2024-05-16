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

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use acdhOeaw\arche\lib\Config;

/**
 * Description of DescriptionTest
 *
 * @author zozlak
 */
class DescriptionTest extends \PHPUnit\Framework\TestCase {

    static private Config $config;
    static private Client $client;
    static private string $baseUrl;

    static public function setUpBeforeClass(): void {
        self::$config  = Config::fromYaml(__DIR__ . '/config.yaml');
        self::$client  = new Client(['http_errors' => false, 'allow_redirects' => false]);
        self::$baseUrl = self::$config->rest->urlBase . self::$config->rest->pathBase;
    }

    public function testYaml(): void {
        $resp = self::$client->send(new Request('get', self::$baseUrl . 'describe'));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('text/vnd.yaml', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0] ?? ''));
        $cfg  = yaml_parse((string) $resp->getBody());
        $this->assertArrayHasKey('schema', $cfg);

        $headers = ['Accept' => 'text/vnd.yaml'];
        $resp    = self::$client->send(new Request('get', self::$baseUrl . 'describe', $headers));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('text/vnd.yaml', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0] ?? ''));
        $cfg     = yaml_parse((string) $resp->getBody());
        $this->assertArrayHasKey('schema', $cfg);
        
        $headers = ['Accept' => 'image/png'];
        $resp    = self::$client->send(new Request('get', self::$baseUrl . 'describe', $headers));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('text/vnd.yaml', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0] ?? ''));
        $cfg     = yaml_parse((string) $resp->getBody());
        $this->assertArrayHasKey('schema', $cfg);        
    }

    public function testJson(): void {
        $headers = ['Accept' => 'application/json'];
        $resp    = self::$client->send(new Request('get', self::$baseUrl . 'describe', $headers));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('application/json', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0] ?? ''));
        $cfg     = json_decode((string) $resp->getBody(), true);
        $this->assertArrayHasKey('schema', $cfg);

        $resp = self::$client->send(new Request('get', self::$baseUrl . 'describe?format=application/json'));
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals('application/json', preg_replace('/;.*$/', '', $resp->getHeader('Content-Type')[0] ?? ''));
        $cfg  = json_decode((string) $resp->getBody(), true);
        $this->assertArrayHasKey('schema', $cfg);
    }

    public function testWrongHttpMethod(): void {
        $resp = self::$client->send(new Request('put', self::$baseUrl . 'describe'));
        $this->assertEquals(405, $resp->getStatusCode());
    }
}
