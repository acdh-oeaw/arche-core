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
use quickRdfIo\RdfIoException;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\RepoDb;
use acdhOeaw\arche\lib\RepoResourceDb;
use acdhOeaw\arche\core\util\TriplesIterator;
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

    private int $id;
    private RepoDb $repo;
    private PDOStatement $pdoStmnt;

    /**
     * Number of triples cached before the RdfNamespace object initialization
     */
    private int $triplesCacheCount = 1000;

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

    /**
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
        $schema         = new Schema(RC::$config->schema);
        $headers        = new Schema(RC::$config->rest->headers);
        $nonRelProp     = RC::$config->metadataManagment->nonRelationProperties;
        $this->repo     = new RepoDb(RC::getBaseUrl(), $schema, $headers, RC::$pdo, $nonRelProp, RC::$auth);
        //$this->repo->setQueryLog(RC::$log);
        $res            = new RepoResourceDb((string) $this->id, $this->repo);
        $this->pdoStmnt = $res->getMetadataStatement($mode, $parentProperty, $resourceProperties, $relativesProperties);
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
        $this->stream = fopen('php://temp', 'rw') ?: throw new RepoException("Failed to open output stream");
        
        if ($format === 'text/html') {
            $serializer = new MetadataGui($this->stream, $this->pdoStmnt, $this->id);
            $serializer->output();
        } else {
            try {
                $serializer = Serializer::getSerializer($format);
            } catch (RdfIoException) {
                throw new RepoException("Unsupported metadata format requested", 400);
            }
            $iter = new TriplesIterator($this->pdoStmnt, RC::getBaseUrl(), RC::$config->schema->id, $this->triplesCacheCount);

            // prepare URI namespace aliases
            $nmsp = $this->getRdfNamespace($iter);

            $serializer->serializeStream($this->stream, $iter, $nmsp);
        }
        unset($this->pdoStmnt);
    }

    private function getRdfNamespace(TriplesIterator $iter): RdfNamespaceInterface {
        $nmsp = new RdfNamespace();
        for ($n = 0; $n < $this->triplesCacheCount - 1 && $iter->valid(); $n++) {
            $quad = $iter->current();
            $nmsp->shorten($quad->getSubject(), true);
            $nmsp->shorten($quad->getPredicate(), true);
            $obj  = $quad->getObject();
            if ($obj instanceof NamedNodeInterface) {
                $nmsp->shorten($obj, true);
            }
            $iter->next();
        }
        $iter->rewind();
        return $nmsp;
    }
}
