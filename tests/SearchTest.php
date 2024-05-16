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
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\SearchTerm;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;

/**
 * Description of TestSearch
 *
 * @author zozlak
 */
class SearchTest extends TestBase {

    /**
     *
     * @var array<DatasetNode>
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
        list($m0, $m1, $m2) = array_map(fn($x) => $x->getNode(), $this->m);
        $prop    = DF::namedNode('https://title');
        $this->m[0]->add(DF::quad($m0, $prop, DF::literal('abc', 'en')));
        $this->m[1]->add(DF::quad($m1, $prop, DF::literal('bcd', 'pl')));
        $this->m[2]->add(DF::quad($m2, $prop, DF::literal('cde')));
        $prop    = DF::namedNode('https://date');
        $this->m[0]->add(DF::quad($m0, $prop, DF::literal('2019-01-01', null, RDF::XSD_DATE)));
        $this->m[1]->add(DF::quad($m1, $prop, DF::literal('2019-02-01', null, RDF::XSD_DATE)));
        $this->m[2]->add(DF::quad($m2, $prop, DF::literal('2019-03-01', null, RDF::XSD_DATE)));
        $prop    = DF::namedNode('https://number');
        $this->m[0]->add(DF::quad($m0, $prop, DF::literal(10)));
        $this->m[1]->add(DF::quad($m1, $prop, DF::literal(2)));
        $this->m[2]->add(DF::quad($m2, $prop, DF::literal(30)));
        $prop    = DF::namedNode('https://relation');
        $this->m[0]->add(DF::quad($m0, $prop, $m2));
        $this->m[1]->add(DF::quad($m1, $prop, $m0));
        $this->m[2]->add(DF::quad($m2, $prop, $m0));
        $this->m[0]->add(DF::quad($m0, DF::namedNode(self::$config->metadataManagment->nonRelationProperties[0]), DF::namedNode('https://test/type')));
        $this->m[1]->add(DF::quad($m1, DF::namedNode('https://title2'), DF::literal('abc')));
        $this->m[0]->add(DF::quad($m0, DF::namedNode(self::$config->spatialSearch->properties[0]), DF::literal('POLYGON((0 0,10 0,10 10,0 10,0 0))')));
        $this->m[1]->add(DF::quad($m1, DF::namedNode(self::$config->spatialSearch->properties[0]), DF::literal('POLYGON((0 0,-10 0,-10 -10,0 -10,0 0))')));
        $this->m[0]->add(DF::quad($m0, DF::namedNode('https://url'), DF::literal('https://foo.bar/baz#bim')));
        $this->m[0]->add(DF::quad($m0, self::$schema->id, DF::namedNode('https://foo.bar/baz#bom')));
        foreach ($this->m as $i) {
            $this->updateResource($i, $txId);
        }
        $this->commitTransaction($txId);
    }

    public function testSimple(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'bcd',
                'property[1]' => 'https://number',
                'value[1]'    => '2',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

    public function testRelationsExplicit(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://relation',
                'value[0]'    => $this->m[0]->getNode()->getValue(),
                'type[0]'     => RDF::RDFS_RESOURCE,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));

        $opts = [
            'query'   => [
                'property[0]' => 'https://relation',
                'value[0]'    => $this->m[0]->getNode()->getValue(),
                'type[0]'     => SearchTerm::TYPE_RELATION,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
    }

    public function testRelationsImplicit(): void {
        $relProp = 'https://relation';

        $opts = [
            'query'   => [
                'property[0]' => $relProp,
                'value[0]'    => $this->m[2]->getNode()->getValue(),
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));

        $opts = [
            'query'   => ['property[]' => $relProp],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));

        $opts = [
            'query'   => [
                'property[]' => $relProp,
                'language[]' => 'en'
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

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
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));

        $opts['query'] = [
            'property[0]' => self::$config->metadataManagment->nonRelationProperties[0],
            'value[0]'    => 'https://test/type',
            'type[0]'     => SearchTerm::TYPE_RELATION,
        ];
        $g             = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));

        $opts['query'] = [
            'property[0]' => self::$config->metadataManagment->nonRelationProperties[0],
            'value[0]'    => 'https://test/type',
            'type[0]'     => RDF::XSD_ANY_URI,
        ];
        $g             = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

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
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

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
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
    }

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
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

    public function testMetaReadCustomMode(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'abc',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => '0_0_1_0',
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode(), self::$schema->searchMatch)));
    }

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
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode(), self::$schema->searchMatch)));
    }

    public function testMetaFilterOutputProperties(): void {
        $opts  = [
            'query'   => [
                'property[0]'            => 'https://title',
                'value[0]'               => 'abc',
                'relativesProperties[0]' => 'https://size',
                'relativesProperties[1]' => 'http://createUser',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode       => '0_1_0_0',
                self::$config->rest->headers->metadataParentProperty => 'https://relation',
                self::$config->rest->headers->resourceProperties     => 'https://mime,https://title',
            ],
        ];
        $g     = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode(), self::$schema->searchMatch)));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode(), self::$schema->searchMatch)));
        $props = $g->listPredicates(new QT($this->m[0]->getNode()))->getValues();
        sort($props);
        $this->assertEquals(['https://mime', 'https://title', 'search://match'], $props);
        $props = $g->listPredicates(new QT($this->m[2]->getNode()))->getValues();
        sort($props);
        $this->assertEquals(['http://createUser', 'https://relation', 'https://size'], $props);
    }

    public function testByLang(): void {
        $countTmpl = new PT(self::$schema->searchCount);
        $opts      = [
            'query'   => [
                'value[0]'    => 'abc',
                'language[0]' => 'en',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g         = $this->runSearch($opts);
        $this->assertEquals(1, $g->getObjectValue($countTmpl));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));

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
        $this->assertEquals(0, $g->getObjectValue($countTmpl));
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));

        $opts = [
            'query'   => [
                'language[0]' => 'pl',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertEquals(1, $g->getObjectValue($countTmpl));
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

    public function testMultipleValues(): void {
        $countTmpl = new PT(self::$schema->searchCount);
        $opts      = [
            'query'   => [
                'value[0][0]' => 'abc',
                'value[0][1]' => 'bcd',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g         = $this->runSearch($opts);
        $this->assertEquals(2, $g->getObjectValue($countTmpl));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
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
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

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
                self::$config->rest->headers->metadataReadMode => '1_0_0_5',
            ],
        ];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(400, $resp->getStatusCode());
        $this->assertEquals('Bad metadata mode 1_0_0_5', (string) $resp->getBody());
    }

    public function testSql(): void {
        $opts = [
            'query'   => [
                'sql' => "SELECT id FROM metadata WHERE value_n = 2",
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
    }

    public function testPaging(): void {
        $countTmpl = new PT(self::$schema->searchCount);
        $valueProp = df::namedNode(self::$config->schema->searchOrderValue . '1');
        $opts      = [
            'query'   => [
                'sql'       => "SELECT id FROM resources",
                'orderBy[]' => '^https://title',
                'limit'     => 1,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g         = $this->runSearch($opts);
        $this->assertEquals(3, $g->getObjectValue($countTmpl));
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals('cde', $g->getObjectValue(new QT($this->m[2]->getNode(), $valueProp)));

        $opts['query']['offset'] = 1;
        $g                       = $this->runSearch($opts);
        $this->assertEquals(3, $g->getObjectValue($countTmpl));
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals('bcd', $g->getObjectValue(new QT($this->m[1]->getNode(), $valueProp)));
    }

    public function testPagingByNumber(): void {
        $countTmpl = new PT(self::$schema->searchCount);
        $valueProp = df::namedNode(self::$config->schema->searchOrderValue . '1');
        $opts      = [
            'query'   => [
                'sql'       => "SELECT id FROM resources",
                'orderBy[]' => 'https://number',
                'limit'     => 1,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g         = $this->runSearch($opts);
        $this->assertEquals(3, $g->getObjectValue($countTmpl));
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals('2', $g->getObjectValue(new QT($this->m[1]->getNode(), $valueProp)));

        $opts['query']['offset'] = 1;
        $g                       = $this->runSearch($opts);
        $this->assertEquals(3, $g->getObjectValue($countTmpl));
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals('10', $g->getObjectValue(new QT($this->m[0]->getNode(), $valueProp)));
    }

    /**
     * @param array<string, string> $expected
     */
    public function testFullTextSearch1(?array $expected = null): void {
        $expected ??= [
            (string) self::$config->schema->id => '<b>verbunden</b>',
            SearchConfig::FTS_BINARY           => "aufs   engste   <b>verbunden</b> .   Auf    kleinasiatischem@Kettenbrücken )   miteinander   <b>verbunden</b> .   Zoll  für@Donautal   <b>verbunden</b> .   Das   Klima  entspricht",
            'http://another/match'             => '<b>verbunden</b>',
        ];

        $ftsValueProp = self::$config->schema->searchFts;
        $ftsPropProp  = self::$config->schema->searchFtsProperty;
        $ftsQueryProp = self::$config->schema->searchFtsQuery;

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
        $meta->add([
            DF::quad($meta->getNode(), DF::namedNode('http://another/match'), DF::literal('foo bar verbunden foo baz')),
            DF::quad($meta->getNode(), self::$schema->id, DF::namedNode('http://verbunden')),
        ]);
        $this->updateResource($meta, $txId);
        $this->commitTransaction($txId);

        // with 2 out of 3 properties matching the search
        $expected = array_slice($expected, 1);
        $opts     = [
            'query'   => [
                'language[]'           => 'en',
                'property[0][0]'       => array_keys($expected)[0],
                'property[0][1]'       => array_keys($expected)[1],
                'value[]'              => 'verbunden',
                'operator[]'           => '@@',
                'ftsMaxFragments'      => 3,
                'ftsFragmentDelimiter' => '@',
                'ftsMinWords'          => 1,
                'ftsMaxWords'          => 5,
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        // with all properties matching the search
        $g        = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $res      = new DatasetNode($meta->getNode(), $g);
        $this->assertGreaterThan(0, count($res));
        for ($i = 1; $res->any(new PT($ftsValueProp . $i)); $i++) {
            $value = $res->getObjectValue(new PT($ftsValueProp . $i));
            $value = str_replace("\n", '', $value);
            $prop  = $res->getObjectValue(new PT($ftsPropProp . $i));
            $this->assertArrayHasKey($prop, $expected);
            $this->assertEquals($expected[$prop], $value);
        }
        $this->assertCount($i - 1, $expected);
        $g   = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $res = new DatasetNode($meta->getNode(), $g);
        $this->assertGreaterThan(0, count($res));
        for ($i = 1; $res->any(new PT($ftsValueProp . $i)); $i++) {
            $value = $res->getObjectValue(new PT($ftsValueProp . $i));
            $value = str_replace("\n", '', $value);
            $prop  = $res->getObjectValue(new PT($ftsPropProp . $i));
            $query = $res->getObjectValue(new PT($ftsQueryProp . $i));
            $this->assertArrayHasKey($prop, $expected);
            $this->assertEquals($expected[$prop], $value);
            $this->assertEquals($opts['query']['value[]'], $query);
        }
        $this->assertCount($i - 1, $expected);
    }

    public function testFullTextSearch2(): void {
        $cfg                                   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $cfg['fullTextSearch']['tikaLocation'] = 'java -Xmx1g -jar ' . __DIR__ . '/../tika/tika-app.jar --text';
        yaml_emit_file(__DIR__ . '/../config.yaml', $cfg);

        $expected = [
            'http://another/match'    => '<b>verbunden</b>',
            self::$config->schema->id => '<b>verbunden</b>',
            SearchConfig::FTS_BINARY  => 'aufs engste <b>verbunden</b> . Auf  kleinasiatischem@Kettenbrücken ) miteinander <b>verbunden</b> . Zoll für@Donautal <b>verbunden</b> . Das Klima entspricht',
        ];
        $this->testFullTextSearch1($expected);
    }

    public function testFullTextSearch3(): void {
        $ftsValueProp = self::$config->schema->searchFts;
        $ftsPropProp  = self::$config->schema->searchFtsProperty;
        $ftsQueryProp = self::$config->schema->searchFtsQuery;
        // by metadata property
        $opts         = [
            'query'   => [
                'property[]' => 'https://title',
                'value[]'    => 'abc',
                'operator[]' => '@@',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g            = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $r            = $this->m[0]->getNode();
        $this->assertEquals("<b>abc</b>", $g->getObjectValue(new QT($r, DF::namedNode($ftsValueProp . '1'))));
        $this->assertEquals("https://title", $g->getObjectValue(new QT($r, DF::namedNode($ftsPropProp . '1'))));
        $this->assertEquals("abc", $g->getObjectValue(new QT($r, DF::namedNode($ftsQueryProp . '1'))));

        // by lang
        $opts = [
            'query'   => [
                'property[]' => 'https://title',
                'language[]' => 'pl',
                'value[]'    => 'bcd',
                'operator[]' => '@@'
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $r    = $this->m[1]->getNode();
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertTrue($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals("<b>bcd</b>", $g->getObjectValue(new QT($r, DF::namedNode($ftsValueProp . '1'))));
        $this->assertEquals("https://title", $g->getObjectValue(new QT($r, DF::namedNode($ftsPropProp . '1'))));
        $this->assertEquals("bcd", $g->getObjectValue(new QT($r, DF::namedNode($ftsQueryProp . '1'))));

        // by property and lang
        $opts = [
            'query'   => [
                'language[]' => 'en',
                'value[]'    => 'abc',
                'operator[]' => '@@'
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $r    = $this->m[0]->getNode();
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertEquals("<b>abc</b>", $g->getObjectValue(new QT($r, DF::namedNode($ftsValueProp . '1'))));
        $this->assertEquals("https://title", $g->getObjectValue(new QT($r, DF::namedNode($ftsPropProp . '1'))));
        $this->assertEquals("abc", $g->getObjectValue(new QT($r, DF::namedNode($ftsQueryProp . '1'))));
    }

    public function testFullTextSearchManual(): void {
        $ftsValueProp = self::$config->schema->searchFts;
        $ftsPropProp  = self::$config->schema->searchFtsProperty;
        $ftsQueryProp = self::$config->schema->searchFtsQuery;
        $idProp       = self::$config->schema->id;
        $txId         = $this->beginTransaction();
        $headers      = [
            self::$config->rest->headers->transactionId => $txId,
            'Content-Disposition'                       => 'attachment; filename="baedeker.xml"',
            'Eppn'                                      => 'admin',
        ];
        $body         = (string) file_get_contents(__DIR__ . '/data/baedeker.xml');
        $req          = new Request('post', self::$baseUrl, $headers, $body);
        $resp         = self::$client->send($req);
        $this->assertEquals(201, $resp->getStatusCode());
        $location     = $resp->getHeader('Location')[0];
        $meta         = $this->extractResource($resp, $location);
        $meta->add([
            DF::quad($meta->getNode(), DF::namedNode('http://another/match'), DF::literal('foo bar verbunden foo baz')),
            DF::quad($meta->getNode(), self::$schema->id, DF::namedNode('http://verbunden')),
        ]);
        $this->updateResource($meta, $txId);
        $this->commitTransaction($txId);

        $opts    = [
            'query'   => [
                'value[]'                 => 'verbunden',
                'operator[]'              => '@@',
                'ftsQuery[0]'             => 'engste',
                'ftsProperty[0]'          => SearchConfig::FTS_BINARY,
                'ftsMaxFragments[0]'      => 2,
                'ftsFragmentDelimiter[0]' => '#',
                'ftsMinWords[0]'          => 1,
                'ftsMaxWords[0]'          => 2,
                'ftsQuery[1]'             => 'verbunden',
                'ftsProperty[1][0]'       => $idProp,
                'ftsProperty[1][1]'       => 'http://another/match',
                'ftsStartSel[1]'          => '%',
                'ftsStopSel[1]'           => '^',
                'ftsFragmentDelimiter[1]' => '#',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g       = $this->runSearch($opts);
        $g       = $this->runSearch($opts);
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertTrue($g->any(new QT($meta->getNode())));
        $res     = new DatasetNode($meta->getNode(), $g);
        $results = [];
        for ($i = 1; $res->any(new PT($ftsValueProp . $i)); $i++) {
            $value          = $res->getObjectValue(new PT($ftsValueProp . $i));
            $value          = str_replace("\n", '', $value);
            $prop           = $res->getObjectValue(new PT($ftsPropProp . $i));
            $query          = $res->getObjectValue(new PT($ftsQueryProp . $i));
            $results[$prop] = ['v' => $value, 'q' => $query];
        }
        $this->assertCount(3, $results);
        $this->assertArrayHasKey(SearchTerm::PROPERTY_BINARY, $results);
        $this->assertArrayHasKey($idProp, $results);
        $this->assertArrayHasKey('http://another/match', $results);
        $binaryExpected = '<b>engste</b> verbunden#<b>engste</b> Stelle';
        $this->assertEquals(['v' => $binaryExpected, 'q' => 'engste'], $results[SearchTerm::PROPERTY_BINARY]);
        $this->assertEquals(['v' => 'http://%verbunden^', 'q' => 'verbunden'], $results[$idProp]);
        $this->assertEquals(['v' => 'foo bar %verbunden^ foo baz', 'q' => 'verbunden'], $results['http://another/match']);
    }

    /**
     * URIs/URLs can be badly parsed by the Postgresql full text search functions.
     * Make sure we are able to properly search for them.
     * @return void
     */
    public function testFullTextSearchIds(): void {
        $countProp = self::$config->schema->searchCount;

        $opts = [
            'query'   => [
                'value[]'    => 'https://foo.bar/baz#bim',
                'operator[]' => '@@',
                'ftsQuery'   => 'https://foo.bar/baz#bim',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_RESOURCE,
            ],
        ];
        $g    = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertEquals(1, $g->getObjectValue(new QT(self::$baseNode)));

        $opts['query']['value[]'] = '"https://foo.bar/baz#bom"';
        $g                        = $this->runSearch($opts);
        $this->assertTrue($g->any(new QT($this->m[0]->getNode())));
        $this->assertEquals(1, $g->getObjectValue(new QT(self::$baseNode)));
    }

    public function testOptions(): void {
        $resp = self::$client->send(new Request('options', self::$baseUrl . 'search'));
        $this->assertEquals('OPTIONS, HEAD, GET, POST', $resp->getHeader('Allow')[0] ?? '');
    }

    public function testVeryOldDate(): void {
        $meta = new DatasetNode(self::$baseNode);
        $meta->add(DF::quad(self::$baseNode, DF::namedNode('https://date'), DF::literal('-12345-01-01', null, RDF::XSD_DATE)));
        $uri  = $this->getResourceMeta($this->createMetadataResource($meta))->getNode();

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
        $this->assertFalse($g->any(new QT($this->m[0]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[1]->getNode())));
        $this->assertFalse($g->any(new QT($this->m[2]->getNode())));
        $this->assertTrue($g->any(new QT($uri)));

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
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[0]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[1]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[2]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($uri))));

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
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[0]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[1]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[2]->getNode()))));
        $this->assertCount(0, $g->copy(new QT($uri)));
    }

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
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[0]->getNode()))));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[1]->getNode()))));
        $this->assertCount(0, $g->copy(new QT($this->m[2]->getNode())));

        // intersects with distance
        $opts['query'] = [
            'operator[0]' => '&&1000',
            'value[0]'    => 'POINT(10.001 10.001)',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[0]->getNode()))));
        $this->assertCount(0, $g->copy(new QT($this->m[1]->getNode())));
        $this->assertCount(0, $g->copy(new QT($this->m[2]->getNode())));

        // db value contains search value
        $opts['query'] = [
            'operator[0]' => '&>',
            'value[0]'    => 'POINT(-5 -5)',
        ];
        $g             = $this->runSearch($opts);
        $this->assertCount(0, $g->copy(new QT($this->m[0]->getNode())));
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[1]->getNode()))));
        $this->assertCount(0, $g->copy(new QT($this->m[2]->getNode())));

        // search value contains db value
        $opts['query'] = [
            'operator[0]' => '&<',
            'value[0]'    => 'POLYGON((-5 -5,-5 10,10 10,10 -5,-5 -5))',
        ];
        $g             = $this->runSearch($opts);
        $this->assertGreaterThan(1, count($g->copy(new QT($this->m[0]->getNode()))));
        $this->assertCount(0, $g->copy(new QT($this->m[1]->getNode())));
        $this->assertCount(0, $g->copy(new QT($this->m[2]->getNode())));
    }

    public function testReadNone(): void {
        $opts = [
            'query'   => [
                'property[0]' => 'https://title',
                'value[0]'    => 'bcd',
                'property[1]' => 'https://number',
                'value[1]'    => '2',
            ],
            'headers' => [
                self::$config->rest->headers->metadataReadMode => RRI::META_NONE,
            ],
        ];
        $resp = self::$client->request('get', self::$baseUrl . 'search', $opts);
        $this->assertEquals(204, $resp->getStatusCode());
        $this->assertEmpty((string) $resp->getBody());
    }

    public function testWrongHttpMethod(): void {
        $resp = self::$client->send(new Request('put', self::$baseUrl . 'search'));
        $this->assertEquals(405, $resp->getStatusCode());
    }
}
