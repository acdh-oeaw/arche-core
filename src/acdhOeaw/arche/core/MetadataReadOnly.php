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

use PDOStatement;
use rdfInterface\NamedNodeInterface;
use rdfInterface\RdfNamespaceInterface;
use simpleRdf\RdfNamespace;
use quickRdfIo\Util as Serializer;
use quickRdfIo\JsonLdStreamSerializer;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\RepoDb;
use acdhOeaw\arche\lib\RepoResourceDb;
use acdhOeaw\arche\core\util\TriplesIterator;
use acdhOeaw\arche\core\RestController as RC;

/**
 * Specialized version of the Metadata class.
 * Supports only reading the metadata but can stream the output assuring small
 * memory footprint.
 * 
 * Uses various serializers depending on the output format.
 * 
 * @author zozlak
 */
class MetadataReadOnly {

    private int $id;
    private string $format;
    private RepoDb $repo;
    private PDOStatement $pdoStmnt;
    private bool $pdoStmntSafe = false;

    /**
     * Parameters of the loadFromDb call stored for lazy initialization.
     * 
     * @var array<string, mixed>
     */
    private array $loadFromDbParams;

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

    public function setFormat(string $format): void {
        $this->format = $format;
    }

    /**
     * Sets up parameters for loading the metadata from the database.
     * 
     * The actual data loading happens when the lazyLoadFromDb() is called.
     * 
     * @param string $mode
     * @param string|null $parentProperty
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @return void
     */
    public function loadFromDb(string $mode, ?string $parentProperty = null,
                               array $resourceProperties = [],
                               array $relativesProperties = []): void {
        $this->loadFromDbParams = [
            'mode'                => $mode,
            'parentProperty'      => $parentProperty,
            'resourceProperties'  => $resourceProperties,
            'relativesProperties' => $relativesProperties,
        ];
    }

    /**
     * 
     * @param RepoDb $repo
     * @param PDOStatement $pdoStatement
     * @param bool $safe set to `true` if the `$pdoStatement` does not require
     *   to be freed on the `freeDbConnection()` call
     * @return void
     */
    public function loadFromPdoStatement(RepoDb $repo,
                                         PDOStatement $pdoStatement,
                                         bool $safe = false): void {
        $this->repo         = $repo;
        $this->pdoStmnt     = $pdoStatement;
        $this->pdoStmntSafe = $safe;
    }

    public function sendOutput(int $triplesCacheCount = 1000): void {
        try {
            if (isset($this->stream)) {
                rewind($this->stream);
                fpassthru($this->stream);
                fclose($this->stream);
            } else {
                RC::$log->debug("Streaming the output");
                $this->stream = fopen('php://output', 'w') ?: throw new RepoException("Failed to open output stream");
                $this->generateOutput($triplesCacheCount);
                fclose($this->stream);
            }
        } catch (\Throwable $ex) {
            http_response_code(500);
            RC::$log->error($ex);
        }
    }

    public function freeDbConnection(): void {
        if (isset($this->pdoStmnt) && !$this->pdoStmntSafe) {
            RC::$log->debug("Materializing output in the memory");
            $this->stream = fopen('php://temp', 'rw') ?: throw new RepoException("Failed to open output stream");
            $this->generateOutput();
        }
    }

    public function lazyLoadFromDb(): void {
        if (isset($this->pdoStmnt) || !isset($this->loadFromDbParams)) {
            return;
        }
        $schema     = new Schema(RC::$config->schema);
        $headers    = new Schema(RC::$config->rest->headers);
        $nonRelProp = RC::$config->metadataManagment->nonRelationProperties;
        $this->repo = new RepoDb(RC::getBaseUrl(), $schema, $headers, RC::$pdo, $nonRelProp, RC::$auth);
        //$this->repo->setQueryLog(RC::$log);

        $res = new RepoResourceDb((string) $this->id, $this->repo);

        $mode           = $this->loadFromDbParams['mode'];
        $parentProp     = $this->loadFromDbParams['parentProperty'];
        $resProps       = $this->loadFromDbParams['resourceProperties'];
        $relProps       = $this->loadFromDbParams['relativesProperties'];
        $this->pdoStmnt = $res->getMetadataStatement($mode, $parentProp, $resProps, $relProps);
        unset($this->loadFromDbParams);
    }

    private function generateOutput(int $triplesCacheCount): void {
        if ($this->format === 'text/html') {
            $serializer = new MetadataGui($this->stream, $this->pdoStmnt, $this->id);
            $serializer->output();
        } else {
            $iter       = new TriplesIterator($this->pdoStmnt, RC::getBaseUrl(), RC::$config->schema->id, $triplesCacheCount);
            $nmsp       = $this->getNamespaces($iter, $triplesCacheCount);
            $largeCount = $iter->key() !== null;
            $iter->sort();
            $iter->rewind();

            $jsonld = ['application/ld+json', 'application/json'];
            if ($largeCount && in_array($this->format, $jsonld)) {
                $serializer = new JsonLdStreamSerializer(JsonLdStreamSerializer::MODE_TRIPLES);
            } else {
                $serializer = Serializer::getSerializer($this->format);
            }
            $serializer->serializeStream($this->stream, $iter, $nmsp);
        }
        unset($this->pdoStmnt);
        unset($this->repo);
    }

    private function getNamespaces(TriplesIterator $iter, int $triplesCacheCount): RdfNamespaceInterface {
        $nmsp = new RdfNamespace();
        for ($n = 0; $n < $triplesCacheCount - 1 && $iter->valid(); $n++) {
            $quad = $iter->current();
            $nmsp->shorten($quad->getSubject(), true);
            $nmsp->shorten($quad->getPredicate(), true);
            $obj  = $quad->getObject();
            if ($obj instanceof NamedNodeInterface) {
                $nmsp->shorten($obj, true);
            }
            $iter->next();
        }
        return $nmsp;
    }
}
