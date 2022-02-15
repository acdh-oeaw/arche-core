<?php

/*
 * The MIT License
 *
 * Copyright 2021 zozlak.
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

namespace acdhOeaw\arche\core;

use PDO;
use PDOStatement;
use pietercolpaert\hardf\TriGWriter;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\RepoDb;
use acdhOeaw\arche\lib\RepoResourceDb;
use acdhOeaw\arche\core\util\Triple;
use acdhOeaw\arche\core\RestController as RC;

/**
 * Specialized version of the Metadata class.
 * Supports only read from database but developed with low memory footprint in mind.
 * 
 * Uses various serializers depending on the output format.
 * 
 * API is a subset of the Metadata class API.
 *
 * @author zozlak
 */
class MetadataReadOnly {

    /**
     * Characters forbidden in n-triples literals according to
     * https://www.w3.org/TR/n-triples/#grammar-production-IRIREF
     *
     * @var string[]
     */
    private static $iriEscapeMap = array(
        "<"    => "\\u003C",
        ">"    => "\\u003E",
        '"'    => "\\u0022",
        "{"    => "\\u007B",
        "}"    => "\\u007D",
        "|"    => "\\u007C",
        "^"    => "\\u005E",
        "`"    => "\\u0060",
        "\\"   => "\\u005C",
        "\x00" => "\\u0000",
        "\x01" => "\\u0001",
        "\x02" => "\\u0002",
        "\x03" => "\\u0003",
        "\x04" => "\\u0004",
        "\x05" => "\\u0005",
        "\x06" => "\\u0006",
        "\x07" => "\\u0007",
        "\x08" => "\\u0008",
        "\x09" => "\\u0009",
        "\x0A" => "\\u000A",
        "\x0B" => "\\u000B",
        "\x0C" => "\\u000C",
        "\x0D" => "\\u000D",
        "\x0E" => "\\u000E",
        "\x0F" => "\\u000F",
        "\x10" => "\\u0010",
        "\x11" => "\\u0011",
        "\x12" => "\\u0012",
        "\x13" => "\\u0013",
        "\x14" => "\\u0014",
        "\x15" => "\\u0015",
        "\x16" => "\\u0016",
        "\x17" => "\\u0017",
        "\x18" => "\\u0018",
        "\x19" => "\\u0019",
        "\x1A" => "\\u001A",
        "\x1B" => "\\u001B",
        "\x1C" => "\\u001C",
        "\x1D" => "\\u001D",
        "\x1E" => "\\u001E",
        "\x1F" => "\\u001F",
        "\x20" => "\\u0020",
    );

    /**
     * Characters forbidden in n-triples literals according to
     * https://www.w3.org/TR/n-triples/#grammar-production-STRING_LITERAL_QUOTE
     * @var string[]
     */
    private static $literalEscapeMap = array(
        "\n" => '\\n',
        "\r" => '\\r',
        '"'  => '\\"',
        '\\' => '\\\\'
    );

    public static function escapeLiteral(string $str): string {
        return strtr($str, self::$literalEscapeMap);
    }

    public static function escapeIri(string $str): string {
        return strtr($str, self::$iriEscapeMap);
    }

    /**
     *
     * @var int
     */
    private $id;

    /**
     * 
     * @var RepoDb
     */
    private $repo;

    /**
     * 
     * @var \PDOStatement
     */
    private $pdoStmnt;

    /**
     * 
     * @var mixed
     */
    private $stream;

    public function __construct(int $id) {
        $this->id = $id;
    }

    public function getUri(): string {
        return RC::getBaseUrl() . $this->id;
    }

    public function loadFromDb(string $mode, ?string $property = null): void {
        $schema         = new Schema(RC::$config->schema);
        $headers        = new Schema(RC::$config->rest->headers);
        $nonRelProp     = RC::$config->metadataManagment->nonRelationProperties;
        $this->repo     = new RepoDb(RC::getBaseUrl(), $schema, $headers, RC::$pdo, $nonRelProp, RC::$auth);
        $res            = new RepoResourceDb((string) $this->id, $this->repo);
        $queryData      = $res->getMetadataQuery($mode, $property);
        $this->pdoStmnt = $this->repo->runQuery($queryData->query, $queryData->param);
    }

    public function loadFromPdoStatement(RepoDb $repo,
                                         PDOStatement $pdoStatement): void {
        $this->repo     = $repo;
        $this->pdoStmnt = $pdoStatement;
    }

    public function sendOutput(): void {
        rewind($this->stream);
        fpassthru($this->stream);
        fclose($this->stream);
    }

    /**
     * 
     * @param string $format
     * @throws RepoException
     */
    public function generateOutput(string $format): void {
        $this->stream = fopen('php://temp', 'rw');
        switch ($format) {
            case 'text/html':
                (new MetadataGui($this->stream, $this->pdoStmnt, $this->id))->output();
                break;
            case 'application/n-triples':
            case 'application/n-quads':
                $this->serializeNTriples();
                break;
            case 'text/turtle':
            case 'application/trig':
                $this->serializeHardf(substr($format, strpos($format, '/') + 1));
                break;
            case 'application/rdf+xml':
            case 'application/xml':
            case 'text/xml':
            case 'application/json':
            case 'application/ld+json':
                $this->serializeEasyRdf($format);
                break;
            default:
                throw new RepoException("Unsupported metadata format requested", 400);
        }
        unset($this->pdoStmnt);
    }

    /**
     * 
     * @param string $format
     * @return void
     */
    private function serializeEasyRdf(string $format): void {
        $graph = $this->repo->parsePdoStatement($this->pdoStmnt);
        fwrite($this->stream, $graph->serialise($format));
    }

    /**
     * 
     * @param string $format
     * @return void
     */
    private function serializeHardf(string $format): void {
        $baseUrl = RC::getBaseUrl();
        $idProp  = RC::$config->schema->id;

        $prefixes = [$baseUrl => 2];
        $data     = [];
        $n        = 1;
        while ($triple   = $this->pdoStmnt->fetchObject(Triple::class)) {
            $data[$triple->id . '.' . $n] = $triple;
            $n++;

            if ($triple->property !== null) {
                $this->addPrefixes($triple->type === 'ID' ? $idProp : $triple->property, $prefixes);
            }
            if ($triple->type === 'ID' || $triple->type === 'URI') {
                $this->addPrefixes($triple->value, $prefixes);
            }
        }
        ksort($data);
        $usePrefixes = [];
        $n           = 1;
        foreach ($prefixes as $k => $v) {
            if ($v > 1) {
                $usePrefixes["ns$n"] = $k;
                $n++;
            }
        }
        unset($prefixes);

        $serializer = new TriGWriter(['format' => $format, 'prefixes' => $usePrefixes]);
        foreach ($data as $triple) {
            list($prop, $obj) = $this->preparePropObj($triple, 'ns1:', $idProp, false);
            $serializer->addTriple('ns1:' . $triple->id, $prop, $obj, null);
            fwrite($this->stream, $serializer->read());
        }
        fwrite($this->stream, (string) $serializer->end());
    }

    /**
     * 
     * @return void
     */
    private function serializeNTriples(): void {
        $baseUrl = self::escapeIri(RC::getBaseUrl());
        $idProp  = self::escapeIri(RC::$config->schema->id);
        while ($triple  = $this->pdoStmnt->fetchObject(Triple::class)) {
            $sbj = $baseUrl . $triple->id;
            list($prop, $obj) = $this->preparePropObj($triple, $baseUrl, $idProp, true);
            fwrite($this->stream, "<$sbj> <$prop> $obj .\n");
        }
    }

    /**
     * 
     * @staticvar int $n
     * @param string $uri
     * @param array<string, int> $prefixes
     * @return void
     */
    private function addPrefixes(string $uri, array &$prefixes): void {
        $p1 = strrpos($uri, '/');
        $p2 = strrpos($uri, '#');
        $p  = max($p1, $p2);
        if ($p > 0 && $p + 1 < strlen($uri)) {
            $prefix            = substr($uri, 0, $p + 1);
            $prefixes[$prefix] = ($prefixes[$prefix] ?? 0) + 1;
        }
    }

    /**
     * 
     * @param Triple $triple
     * @param string $baseUrl
     * @param string $idProp
     * @param bool $ntriples
     * @return array<string>
     */
    private function preparePropObj(Triple $triple, string $baseUrl,
                                    string $idProp, bool $ntriples): array {
        $literal = !in_array($triple->type, ['ID', 'REL', 'URI']);
        if ($triple->type === 'ID') {
            $triple->property = $idProp;
        } elseif ($triple->type === 'REL') {
            $triple->value = $baseUrl . $triple->value;
        }

        if ($ntriples) {
            $triple->property = self::escapeIri((string) $triple->property);
            if ($literal) {
                $triple->value = self::escapeLiteral((string) $triple->value);
                $triple->type  = '<' . self::escapeIri((string) $triple->type) . '>';
            } else {
                $triple->value = '<' . self::escapeIri((string) $triple->value) . '>';
            }
        }

        if ($literal) {
            $obj = '"' . $triple->value . '"';
            $obj .= empty($triple->lang) ? '' : "@" . $triple->lang;
            $obj .= empty($triple->lang) && $triple->type !== RDF::XSD_STRING ? '^^' . $triple->type : '';
        } else {
            $obj = $triple->value;
        }
        return [(string) $triple->property, (string) $obj];
    }
}
