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
use EasyRdf\Resource;
use EasyRdf\Literal;
use GuzzleHttp\Psr7\Request;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\SearchTerm;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;

/**
 * Description of TestSearch
 *
 * @author zozlak
 */
class SearchTest extends TestBase {

    /**
     *
     * @var array<Resource>
     */
    private array $m;

    public function setUp(): void {
        parent::setUp();

        $txId    = $this->beginTransaction();
        $this->m = [
            $this->getResourceMeta($this->createBinaryResource($txId)),
            $this->getResourceMeta($this->createBinaryResource($txId)),
            $this->getResourceMeta($this->createBinaryResource($txId)),
        ];
        $this->m[0]->addLiteral('https://title', new Literal('abc', 'en'));
        $this->m[1]->addLiteral('https://title', new Literal('bcd', 'pl'));
        $this->m[2]->addLiteral('https://title', 'cde');
        $this->m[0]->addLiteral('https://date', new Literal('2019-01-01', null, RDF::XSD_DATE));
        $this->m[1]->addLiteral('https://date', new Literal('2019-02-01', null, RDF::XSD_DATE));
        $this->m[2]->addLiteral('https://date', new Literal('2019-03-01', null, RDF::XSD_DATE));
        $this->m[0]->addLiteral('https://number', 10);
        $this->m[1]->addLiteral('https://number', 20);
        $this->m[2]->addLiteral('https://number', 30);
        $this->m[0]->addResource('https://relation', $this->m[2]->getUri());
        $this->m[1]->addResource('https://relation', $this->m[0]->getUri());
        $this->m[2]->addResource('https://relation', $this->m[0]->getUri());
        $this->m[0]->addResource(self::$config->metadataManagment->nonRelationProperties[0], 'https://test/type');
        $this->m[1]->addLiteral('https://title2', 'abc');
        $this->m[0]->addLiteral(self::$config->spatialSearch->properties[0], 'POLYGON((0 0,10 0,10 10,0 10,0 0))');
        $this->m[1]->addLiteral(self::$config->spatialSearch->properties[0], 'POLYGON((0 0,-10 0,-10 -10,0 -10,0 0))');
        foreach ($this->m as $i) {
            $this->updateResource($i, $txId);
        }
        $this->commitTransaction($txId);
    }

    /**
     * @group search
     */
    public function testSimple(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'bcd',
                'property[1]' => 'https://number',
                'value[1]'    => '20',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testRelationsExplicit(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://relation',
                'value[0]'    => $this->m[0]->getUri(),
                'type[0]'     => RDF::RDFS_RESOURCE,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts = [
            'query'   => [
                'property[0]' => 'https://relation',
                'value[0]'    => $this->m[0]->getUri(),
                'type[0]'     => SearchTerm::TYPE_RELATION,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testRelationsImplicit(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://relation',
                'value[0]'    => $this->m[2]->getUri(),
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testLiteralUri(): void {
        $opts = [
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];

        $opts['query'] = [
            'property[0]' => self::$config->metadataManagment->nonRelationProperties[0],
            'value[0]'    => 'https://test/type',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts['query'] = [
            'property[0]' => self::$config->metadataManagment->nonRelationProperties[0],
            'value[0]'    => 'https://test/type',
            'type[0]'     => SearchTerm::TYPE_RELATION,
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts['query'] = [
            'property[0]' => self::$config->metadataManagment->nonRelationProperties[0],
            'value[0]'    => 'https://test/type',
            'type[0]'     => RDF::XSD_ANY_URI,
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testByDateImplicit(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://date',
                'value[0]'    => '2019-02-01',
                'operator[0]' => '<=',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testByDateExplicit(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://date',
                'value[0]'    => '2019-02-01',
                'type[0]'     => RDF::XSD_DATE,
                'operator[0]' => '>=',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testRegex(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'bc',
                'operator[0]' => '~',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testMetaReadNeigbors(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'abc',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_NEIGHBORS,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertTrue($g->resource($this->m[0])->getLiteral(self::$config->schema->searchMatch)?->getValue());
        $this->assertNull($g->resource($this->m[2])->getLiteral(self::$config->schema->searchMatch));
    }

    /**
     * @group search
     */
    public function testMetaReadRelatives(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'abc',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode       => 'relatives',
                self::$config->rest->headers->metadataParentProperty => 'https://relation',
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertTrue($g->resource($this->m[0])->getLiteral(self::$config->schema->searchMatch)?->getValue());
        $this->assertNull($g->resource($this->m[1])->getLiteral(self::$config->schema->searchMatch));
        $this->assertNull($g->resource($this->m[2])->getLiteral(self::$config->schema->searchMatch));
    }

    /**
     * @group search
     */
    public function testByLang(): void {
        $opts = [
            'query'   => [
                'value[0]'    => 'abc',
                'language[0]' => 'en',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(1, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts = [
            'query'   => [
                'value[0]'    => 'abc',
                'language[0]' => 'pl',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts = [
            'query'   => [
                'language[0]' => 'pl',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(1, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    public function testMultipleValues(): void {
        $opts = [
            'query'   => [
                'value[0][0]' => 'abc',
                'value[0][1]' => 'bcd',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(2, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    public function testMultipleProperties(): void {
        $opts = [
            'query'   => [
                'value[]'        => 'abc',
                'property[0][0]' => 'https://title',
                'property[0][1]' => 'https://title2',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testExceptions(): void {
        $opts = ['query' => [
                'value[0]'    => 'abc',
                'operator[0]' => 'foo',
        ]];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Unknown operator foo', (string) $resp->getBody());

        $opts = ['query' => [
                'value[0]' => 'abc',
                'type[0]'  => 'foo',
        ]];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Unknown type foo', (string) $resp->getBody());

        $opts = ['query' => [
                'value[0]' => '',
        ]];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Empty search term', (string) $resp->getBody());

        $opts = ['query' => [
                'value[0]' => '',
                'type[0]'  => RDF::XSD_ANY_URI,
        ]];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Empty search term', (string) $resp->getBody());

        $opts = ['query' => [
                'sql' => 'wrong query',
        ]];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Bad query', (string) $resp->getBody());

        $opts = [
            'query'   => [
                'value[0]' => 'abc',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => 'foo',
            ],
        ];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Wrong metadata read mode value foo', (string) $resp->getBody());
    }

    /**
     * @group search
     */
    public function testSql(): void {
        $opts = [
            'query'   => [
                'sql' => "SELECT id FROM metadata WHERE value_n = 20",
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testPaging(): void {
        $opts = [
            'query'   => [
                'sql'       => "SELECT id FROM resources",
                'orderBy[]' => '^https://title',
                'limit'     => 1,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(3, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        $opts['query']['offset'] = 1;
        $g                       = $this->runSearch($opts);
        $this->assertEquals(3, $g->resource(self::$baseUrl)->getLiteral(self::$config->schema->searchCount)?->getValue());
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testFullTextSearch1(string $result = null): void {
        $cfg = yaml_parse_file(__DIR__ . '/../config.yaml');
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);
        self::reloadTxCtrlConfig();

        $txId     = $this->beginTransaction();
        $headers  = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Disposition'                       => 'attachment; filename="baedeker.xml"',
            'Eppn'                                      => 'admin',
        ];
        $body     = (string) file_get_contents(__DIR__ . '/data/baedeker.xml');
        $req      = new Request('post', self::$baseUrl, $headers, $body);
        $resp     = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location = $resp->getHeader('Location')[0];
        $meta     = $this->extractResource($resp, $location);
        $this->commitTransaction($txId);

        $opts = [
            'query'   => [
                'language[]'           => 'en',
                'property[]'           => SearchTerm::PROPERTY_BINARY,
                'value[]'              => 'verbunden',
                'operator[]'           => '@@',
                'ftsQuery'             => 'verbunden',
                'ftsProperty'          => SearchTerm::PROPERTY_BINARY,
                'ftsMaxFragments'      => 3,
                'ftsFragmentDelimiter' => '@',
                'ftsMinWords'          => 1,
                'ftsMaxWords'          => 5,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($meta->getUri())->propertyUris()));

        $fts = (string) $g->resource($meta->getUri())->getLiteral(self::$config->schema->searchFts);
        $fts = str_replace("\n", '', $fts);
        $this->assertEquals($result ?? "aufs   engste   <b>verbunden</b> .   Auf    kleinasiatischem@Kettenbrücken )   miteinander   <b>verbunden</b> .   Zoll  für@Donautal   <b>verbunden</b> .   Das   Klima  entspricht", $fts);
    }

    public function testFullTextSearch2(): void {
        $cfg                                   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['fullTextSearch']['tikaLocation'] = 'java -Xmx1g -jar ' . __DIR__ . '/../tika/tika-app.jar --text';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $this->testFullTextSearch1('aufs engste <b>verbunden</b> . Auf  kleinasiatischem@Kettenbrücken ) miteinander <b>verbunden</b> . Zoll für@Donautal <b>verbunden</b> . Das Klima entspricht');
    }

    public function testFullTextSearch3(): void {
        // by metadata property
        $opts = [
            'query'   => [
                'property[]'  => 'https://title',
                'value[]'     => 'abc',
                'operator[]'  => '@@',
                'ftsQuery'    => 'abc',
                'ftsProperty' => 'https://title',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $fts  = (string) $g->resource($this->m[0]->getUri())->getLiteral(self::$config->schema->searchFts);
        $this->assertEquals("<b>abc</b>", $fts);

        // by lang
        $opts = [
            'query'   => [
                'property[]'  => 'https://title',
                'language[]'  => 'pl',
                'value[]'     => 'bcd',
                'operator[]'  => '@@',
                'ftsQuery'    => 'bcd',
                'ftsProperty' => 'https://title',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $fts  = (string) $g->resource($this->m[1]->getUri())->getLiteral(self::$config->schema->searchFts);
        $this->assertEquals("<b>bcd</b>", $fts);

        // by property and lang
        $opts = [
            'query'   => [
                'language[]'  => 'en',
                'value[]'     => 'abc',
                'operator[]'  => '@@',
                'ftsQuery'    => 'abc',
                'ftsProperty' => 'https://title',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $fts  = (string) $g->resource($this->m[0]->getUri())->getLiteral(self::$config->schema->searchFts);
        $this->assertEquals("<b>abc</b>", $fts);
    }

    public function testFullTextSearchIds(): void {
        $countProp = self::$config->schema->searchCount;
        $opts = [
            'query'   => [
                'value[]'     => $this->m[0]->getUri(),
                'operator[]'  => '@@',
                'ftsQuery'    => $this->m[0]->getUri(),
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(1, $g->resource(self::$baseUrl)->getLiteral($countProp)->getValue());
    }
    
    /**
     * @group search
     */
    public function testOptions(): void {
        $resp = self::$client->send(new Request('options', self::$baseUrl . 'search'));
        $this->assertEquals('OPTIONS, HEAD, GET, POST', $resp->getHeader('Allow')[0] ?? '');
    }

    /**
     * @group search
     */
    public function testVeryOldDate(): void {
        $meta = (new Graph())->resource(self::$baseUrl);
        $meta->addLiteral('https://date', new Literal('-12345-01-01', null, RDF::XSD_DATE));
        $uri  = $this->getResourceMeta($this->createMetadataResource($meta));

        $opts = [
            'query'   => [
                'property[0]' => 'https://date',
                'value[0]'    => '2000-02-01',
                'operator[0]' => '<=',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($uri)->propertyUris()));

        $opts = [
            'query'   => [
                'property[0]' => 'https://date',
                'value[0]'    => '-12346-12-31',
                'type[0]'     => RDF::XSD_DATE,
                'operator[0]' => '>=',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($uri)->propertyUris()));

        $opts = [
            'query'   => [
                'property[0]' => 'https://date',
                'value[0]'    => '-0100-12-31',
                'operator[0]' => '>=',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[2]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($uri)->propertyUris()));
    }

    /**
     * @group search
     */
    public function testSpatial(): void {
        // m[0]: POLYGON((0 0,10 0,10 10,0 10,0 0))
        // m[1]: POLYGON((0 0,-10 0,-10 -10,0 -10,0 0))
        $opts = ['headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
        ]];

        // intersects
        $opts['query'] = [
            'operator[0]' => '&&',
            'value[0]'    => 'POINT(0 0)',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        // intersects with distance
        $opts['query'] = [
            'operator[0]' => '&&1000',
            'value[0]'    => 'POINT(10.001 10.001)',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        // db value contains search value
        $opts['query'] = [
            'operator[0]' => '&>',
            'value[0]'    => 'POINT(-5 -5)',
        ];
        $g             = $this->runSearch($opts);
        $this->assertEquals(0, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertGreaterThan(1, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));

        // search value contains db value
        $opts['query'] = [
            'operator[0]' => '&<',
            'value[0]'    => 'POLYGON((-5 -5,-5 10,10 10,10 -5,-5 -5))',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->resource($this->m[0]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[1]->getUri())->propertyUris()));
        $this->assertEquals(0, count($g->resource($this->m[2]->getUri())->propertyUris()));
    }

    /**
     * @group search
     */
    public function testReadNone(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'bcd',
                'property[1]' => 'https://number',
                'value[1]'    => '20',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            ],
        ];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(204, $resp->getStatusCode());
        $this->assertEmpty((string) $resp->getBody());
    }

    /**
     * @group search
     */
    public function testWrongHttpMethod(): void {
        $resp = self::$client->send(new Request('put', self::$baseUrl . 'search'));
        $this->assertEquals(405, $resp->getStatusCode());
    }
}
