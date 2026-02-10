<?php

/*
 * The MIT License
 *
 * Copyright 2024 zozlak.
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

use ZipArchive;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use quickRdf\Dataset;
use quickRdf\DataFactory as DF;
use quickRdfIo\NQuadsSerializer;

/**
 * Description of DownloadTest
 *
 * @author zozlak
 */
class DownloadTest extends TestBase {

    const TMP_ZIP = __DIR__ . '/data.zip';

    public function tearDown(): void {
        parent::tearDown();

        if (file_exists(self::TMP_ZIP)) {
            unlink(self::TMP_ZIP);
        }
    }

    public function testSingleBinary(): void {
        $uri        = $this->createBinaryResource();
        $dwnldUri   = self::$baseUrl . 'download?ids=' . preg_replace('`^.*/`', '', $uri);
        $req        = new Request('get', $dwnldUri, ['eppn' => 'admin']);
        $resp       = self::$client->send($req);
        $content    = $this->testZipBasics($resp, 1);
        $refContent = [basename(self::BINARY_RES_PATH) => file_get_contents(self::BINARY_RES_PATH)];
        $this->assertEquals($refContent, $content);
    }

    public function testCollection(): void {
        // create resource structure
        $serializer  = new NQuadsSerializer();
        $txId        = $this->beginTransaction();
        $headers     = [
            'eppn'                                      => 'admin',
            'content-type'                              => 'application/n-triples',
            self::$config->rest->headers->transactionId => $txId,
        ];
        $collections = [];
        $binaries    = [];
        for ($i = 0; $i < 3; $i++) {
            $sbj  = DF::namedNode(self::$baseUrl . '/metadata');
            $meta = new Dataset();
            $meta->add(DF::quad($sbj, self::$schema->label, DF::literal("collection $i", "en")));
            if ($i > 0) {
                $meta->add(DF::quad($sbj, self::$schema->parent, $collections[$i - 1]));
            }
            $collections[$i] = DF::namedNode($this->createMetadataResource($meta, $txId));
            for ($j = 0; $j < 2; $j++) {
                $uri            = $this->createBinaryResource($txId, $j === 0 ? self::BINARY_RES_PATH : __FILE__);
                $binaries[$i][] = $uri;
                $sbj            = DF::namedNode($uri);
                $meta           = new Dataset();
                $meta->add(DF::quad($sbj, self::$schema->parent, $collections[$i]));
                $req            = new Request('patch', "$uri/metadata", $headers, $serializer->serialize($meta));
                $resp           = self::$client->send($req);
                $this->assertEquals(200, $resp->getStatusCode());
            }
        }
        $this->commitTransaction($txId);

        // two arbitrary binaries using GET
        $ids        = [self::$baseUrl . $binaries[2][1], $binaries[0][0]];
        $ids        = array_map(fn($x) => preg_replace('`^.*/`', '', $x), $ids);
        $uri        = self::$baseUrl . 'download?' . http_build_query(['ids' => $ids]);
        $req        = new Request('get', $uri, ['eppn' => 'admin']);
        $resp       = self::$client->send($req);
        $content    = $this->testZipBasics($resp, 2);
        $refContent = [
            'collection 0/test.ttl'                                   => file_get_contents(self::BINARY_RES_PATH),
            'collection 0/collection 1/collection 2/DownloadTest.php' => file_get_contents(__FILE__),
        ];
        $this->assertEquals($refContent, $content);

        // lowest collection using GET
        $ids        = [(string) $collections[2]];
        $ids        = array_map(fn($x) => preg_replace('`^.*/`', '', $x), $ids);
        $uri        = self::$baseUrl . 'download?' . http_build_query(['ids' => $ids]);
        $req        = new Request('get', $uri, ['eppn' => 'admin']);
        $resp       = self::$client->send($req);
        $content    = $this->testZipBasics($resp, 2);
        $refContent = [
            'collection 0/collection 1/collection 2/test.ttl'         => file_get_contents(self::BINARY_RES_PATH),
            'collection 0/collection 1/collection 2/DownloadTest.php' => file_get_contents(__FILE__),
        ];

        // middle collection with arbitrary files using POST
        $ids        = [(string) $collections[1], $binaries[0][1], $binaries[2][0]];
        $ids        = array_map(fn($x) => preg_replace('`^.*/`', '', $x), $ids);
        $uri        = self::$baseUrl . 'download';
        $headers    = ['eppn' => 'admin', 'content-type' => 'application/x-www-form-urlencoded'];
        $req        = new Request('post', $uri, $headers, http_build_query(['ids' => $ids]));
        $resp       = self::$client->send($req);
        $content    = $this->testZipBasics($resp, 5);
        $refContent = [
            'collection 0/DownloadTest.php'                           => file_get_contents(__FILE__),
            'collection 0/collection 1/test.ttl'                      => file_get_contents(self::BINARY_RES_PATH),
            'collection 0/collection 1/DownloadTest.php'              => file_get_contents(__FILE__),
            'collection 0/collection 1/collection 2/test.ttl'         => file_get_contents(self::BINARY_RES_PATH),
            'collection 0/collection 1/collection 2/DownloadTest.php' => file_get_contents(__FILE__),
        ];
    }

    public function testAuth(): void {
        // create resource structure
        $username   = 'ordinaryUser';
        $serializer = new NQuadsSerializer();
        $txId       = $this->beginTransaction();
        $headers    = [
            'eppn'                                      => 'admin',
            'content-type'                              => 'application/n-triples',
            self::$config->rest->headers->transactionId => $txId,
        ];
        $binaries   = [];
        $sbj        = DF::namedNode(self::$baseUrl . '/metadata');
        $meta       = new Dataset();
        $meta->add(DF::quad($sbj, self::$schema->label, DF::literal("collection_1->: ąę", "en")));
        $collection = DF::namedNode($this->createMetadataResource($meta, $txId));
        for ($j = 0; $j < 2; $j++) {
            $uri        = $this->createBinaryResource($txId, $j === 0 ? self::BINARY_RES_PATH : __FILE__);
            $binaries[] = $uri;
            $sbj        = DF::namedNode($uri);
            $meta       = new Dataset();
            $meta->add(DF::quad($sbj, self::$schema->parent, $collection));
            if ($j === 0) {
                $meta->add(DF::quad($sbj, DF::namedNode(self::$config->accessControl->schema->read), DF::literal($username)));
            }
            $req  = new Request('patch', "$uri/metadata", $headers, $serializer->serialize($meta));
            $resp = self::$client->send($req);
            $this->assertEquals(200, $resp->getStatusCode());
        }
        $this->commitTransaction($txId);

        $ids = [(string) $collection];
        $ids = array_map(fn($x) => preg_replace('`^.*/`', '', $x), $ids);

        // without skipping unauthorized
        $uri  = self::$baseUrl . 'download?' . http_build_query(['ids' => $ids]);
        $req  = new Request('get', $uri);
        $resp = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
        $req  = new Request('get', $uri, ['eppn' => $username]);
        $resp = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());

        // with skipping unauthorized
        $param      = ['ids' => $ids, 'skipUnauthorized' => true];
        $uri        = self::$baseUrl . 'download?' . http_build_query($param);
        $req        = new Request('get', $uri);
        $resp       = self::$client->send($req);
        $this->assertEquals(403, $resp->getStatusCode());
        $this->assertEquals("Unauthorized to download all requested resources", (string) $resp->getBody());
        $req        = new Request('get', $uri, ['eppn' => $username]);
        $resp       = self::$client->send($req);
        $content    = $this->testZipBasics($resp, 1);
        $refContent = ['collection_1-__ ąę/test.ttl' => file_get_contents(self::BINARY_RES_PATH)];
        $this->assertEquals($refContent, $content);
    }

    /**
     * 
     * @return array<string>
     */
    private function testZipBasics(ResponseInterface $resp, int $expectedCount): array {
        $this->assertEquals(200, $resp->getStatusCode());
        $this->assertEquals(["attachment; filename*=UTF-8''data.zip"], $resp->getHeader('Content-Disposition'));
        $this->assertEquals(['application/zip'], $resp->getHeader('Content-Type'));
        file_put_contents(self::TMP_ZIP, (string) $resp->getBody());
        $zip     = new ZipArchive();
        $this->assertTrue($zip->open(self::TMP_ZIP));
        $this->assertEquals($expectedCount, $zip->count());
        $content = [];
        for ($i = 0; $i < $expectedCount; $i++) {
            $content[$zip->getNameIndex($i)] = $zip->getFromIndex($i);
        }
        ksort($content);
        return $content;
    }
}
