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

namespace acdhOeaw\arche\core;

use DateTime;
use PDOException;
use zozlak\HttpAccept;
use zozlak\RdfConstants as RDF;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\LiteralInterface;
use rdfInterface\NamedNodeInterface;
use quickRdf\DatasetNode;
use quickRdf\NamedNode;
use quickRdf\Literal;
use quickRdf\Quad;
use quickRdf\DataFactory as DF;
use quickRdfIo\Util as RdfUtil;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\RepoDb;
use acdhOeaw\arche\lib\RepoResourceDb;
use acdhOeaw\arche\lib\RepoResourceInterface AS RRI;

/**
 * Manages resources's metadata (loads from database or HTTP request, writes into
 * the database, serializes to RDF, etc.).
 *
 * @author zozlak
 */
class Metadata {

    const TYPE_GEOM      = 'GEOM';
    const TYPE_URI       = 'URI';
    const SAVE_ADD       = 'add';
    const SAVE_OVERWRITE = 'overwrite';
    const SAVE_MERGE     = 'merge';
    const FILTER_SKIP    = 'skip';
    const FILTER_INCLUDE = 'include';
    const NUMERIC_TYPES  = [RDF::XSD_DECIMAL, RDF::XSD_FLOAT, RDF::XSD_DOUBLE, RDF::XSD_INTEGER,
        RDF::XSD_NEGATIVE_INTEGER, RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_NON_POSITIVE_INTEGER,
        RDF::XSD_POSITIVE_INTEGER, RDF::XSD_LONG, RDF::XSD_INT, RDF::XSD_SHORT, RDF::XSD_BYTE,
        RDF::XSD_UNSIGNED_LONG, RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE,
    ];
    const DATE_TYPES     = [RDF::XSD_DATE, RDF::XSD_DATE_TIME];

    static public function idAsUri(int $id): string {
        return RC::getBaseUrl() . $id;
    }

    static public function getAcceptedFormats(): string {
        return implode(', ', [
            'application/n-triples;q=1',
            'application/json;q=0.5',
            'application/ld+json;q=0.5',
            'application/rdf+n3;q=0.5',
            'application/trig;q=0.5',
            'application/turtle;q=0.5',
            'application/n-quads;q=0.5',
            'application/rdf+xml;q=0.5',
            'application/xml;q=0.5',
            'text/n3;q=0.5',
            'text/rdf;q=0.5',
            'text/rdf+n3;q=0.5',
            'text/turtle;q=0.5',
        ]);
    }

    static public function negotiateFormat(): string {
        $format = filter_input(\INPUT_GET, 'format');
        if (!empty($format)) {
            if (!in_array($format, RC::$config->rest->metadataFormats)) {
                throw new RepoException('Unsupported metadata format requested', 400);
            }
            return $format;
        }
        return HttpAccept::getBestMatch(RC::$config->rest->metadataFormats)->getFullType();
    }

    private int $id;
    private DatasetNodeInterface $graph;

    public function __construct(?int $id = null) {
        if ($id !== null) {
            $this->id    = $id;
            $this->graph = new DatasetNode(DF::namedNode($this->getUri()));
        }
    }

    public function setId(int $id): void {
        $this->id = $id;
        if (isset($this->graph)) {
            $node        = DF::namedNode($this->getUri());
            $this->graph->forEach(fn(Quad $x) => $x->withSubject($node));
            $this->graph = $this->graph->withNode($node);
        }
    }

    public function getUri(): string {
        return isset($this->id) ? self::idAsUri($this->id) : RC::getBaseUrl();
    }

    public function getDatasetNode(): DatasetNodeInterface {
        return $this->graph;
    }

    /**
     * 
     * @param DatasetNode $newMeta
     * @param array<string> $preserve
     * @return void
     */
    public function update(DatasetNode $newMeta, array $preserve = []): void {
        $node = $this->graph->getNode();
        foreach ($newMeta->listPredicates() as $predicate) {
            if (!in_array($predicate->getValue(), $preserve)) {
                $this->graph->delete(new PT($predicate));
            }
        }
        $this->graph->add($newMeta->map(fn($x) => $x->withSubject($node))->withNode($node));
    }

    public function loadFromRequest(string $resUri = null): int {
        $format      = filter_input(INPUT_SERVER, 'CONTENT_TYPE');
        $length      = (int) filter_input(INPUT_SERVER, 'CONTENT_LENGTH');
        $node        = DF::namedNode($this->getUri());
        $this->graph = new DatasetNode($node);
        if ($length > 0) {
            if (empty($format)) {
                $format = 'application/n-triples';
            }
            $this->graph->add(RdfUtil::parse(fopen('php://input', 'r'), new DF(), $format));
        }

        if (!empty($resUri)) {
            $filter = new QT(DF::namedNode($resUri));
            $this->graph->getDataset()->forEach(fn($x) => $x->withSubject($node), $filter);
        }
        if (count($this->graph) === 0) {
            RC::$log->warning("No metadata for $node \n" . RdfUtil::serialize($this->graph->getDataset(), 'text/turtle'));
        }
        return count($this->graph);
    }

    public function loadFromResource(DatasetNodeInterface $res): void {
        $this->graph = $res;
    }

    public function loadFromDb(string $mode, ?string $property = null): void {
        $schema      = new Schema(RC::$config->schema);
        $headers     = new Schema(RC::$config->rest->headers);
        $nonRelProp  = RC::$config->metadataManagment->nonRelationProperties;
        $repo        = new RepoDb(RC::getBaseUrl(), $schema, $headers, RC::$pdo, $nonRelProp, RC::$auth);
        $res         = new RepoResourceDb((string) $this->id, $repo);
        $res->loadMetadata(true, $mode, $property);
        $this->graph = $res->getGraph();
    }

    public function merge(string $mode): DatasetNodeInterface {
        switch ($mode) {
            case self::SAVE_ADD:
                RC::$log->debug("\tadding metadata");
                $meta = new Metadata($this->id);
                $meta->loadFromDb(RRI::META_RESOURCE);
                $meta = $meta->getDatasetNode();
                $meta->add($this->graph);
                break;
            case self::SAVE_MERGE:
                RC::$log->debug("\tmerging metadata");
                $meta = new Metadata($this->id);
                $meta->loadFromDb(RRI::META_RESOURCE);
                $meta = $meta->getDatasetNode();
                foreach ($this->graph->listPredicates()->skip([RC::$schema->id]) as $predicate) {
                    $meta->delete(new PT($predicate));
                }
                $meta->add($this->graph);
                break;
            case self::SAVE_OVERWRITE:
                RC::$log->debug("\toverwriting metadata");
                $meta = $this->graph;
                break;
            default:
                throw new RepoException('Wrong metadata merge mode ' . $mode, 400);
        }
        $this->manageSystemMetadata($meta);
        RC::$log->debug("\n" . RdfUtil::serialize($meta, 'text/turtle'));

        $this->graph = $meta;
        return $meta;
    }

    public function save(): void {
        $spatialProps = RC::$config->spatialSearch->properties ?? [];
        $query        = RC::$pdo->prepare("DELETE FROM metadata WHERE id = ?");
        $query->execute([$this->id]);
        $query        = RC::$pdo->prepare("DELETE FROM relations WHERE id = ?");
        $query->execute([$this->id]);

        try {
            $queryV     = RC::$pdo->prepare("INSERT INTO metadata (id, property, type, lang, value_n, value_t, value) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING mid");
            $queryI     = RC::$pdo->prepare("INSERT INTO identifiers (id, ids) VALUES (?, ?)");
            // deal with the problem of multiple identifiers leading to same rows in the relations table
            $queryR     = RC::$pdo->prepare("
                INSERT INTO relations (id, target_id, property) 
                SELECT ?, id, ? FROM identifiers WHERE ids = ? 
                ON CONFLICT (id, target_id, property) DO UPDATE SET id = excluded.id
            ");
            $queryRSelf = RC::$pdo->prepare("
                INSERT INTO relations (id, target_id, property) 
                VALUES (?, ?, ?) 
                ON CONFLICT (id, target_id, property) DO NOTHING
            ");
            $allButIds  = $this->graph->getIterator(new PT(RC::$schema->id, negate: true));
            $ids        = $this->graph->listObjects(new PT(RC::$schema->id))->getValues();
            foreach ($allButIds as $triple) {
                $p = $triple->getPredicate()->getValue();
                $v = $triple->getObject();
                if (!$v instanceof LiteralInterface && in_array($p, RC::$config->metadataManagment->nonRelationProperties)) {
                    $v = DF::literal($triple->getObject()->getValue(), null, self::TYPE_URI);
                }

                if ($v instanceof NamedNodeInterface) {
                    $v = $v->getValue();
                    RC::$log->debug("\tadding relation " . $p . " " . $v);
                    // $v may not exist in identifiers table yet, thus special handling
                    if (in_array($v, $ids)) {
                        $queryRSelf->execute([$this->id, $this->id, $p]);
                    } else {
                        $queryR->execute([$this->id, $p, $v]);
                        if ($queryR->rowCount() === 0) {
                            $added = $this->autoAddId($v);
                            if ($added) {
                                $queryR->execute([$this->id, $p, $v]);
                            }
                        }
                    }
                } elseif ($v instanceof LiteralInterface) {
                    $vv = $v->getValue();
                    if (in_array($p, $spatialProps)) {
                        $v = $v->withDatatype(self::TYPE_GEOM);
                    }
                    $type = $v->getDatatype();
                    if (in_array($type, self::NUMERIC_TYPES)) {
                        $param = [$this->id, $p, $type, '', $vv, null, $vv];
                    } else if (in_array($type, self::DATE_TYPES)) {
                        $vt = $vv;
                        $vn = (int) $vt;
                        ;
                        if (substr($vt, 0, 1) === '-') {
                            // Postgresql doesn't parse BC dates in xsd:date but the transformation 
                            // is simple as in xsd:date there is no 0 year (in contrary to ISO)
                            $vt = substr($vt, 1) . ' BC';
                            if ($vn < -4713) {
                                $vt = null;
                            }
                        }
                        $param = [$this->id, $p, $type, '', $vn, $vt, $vv];
                    } else {
                        $lang  = $v->getLang() ?? '';
                        $param = [$this->id, $p, $type, $lang, null, null, $vv];
                    }
                    $queryV->execute($param);
                }
            }

            // Postpone processing ids because it would lock identifiers db table
            // and as a consequence prevent Transaction::createResource() from working
            // while Transaction::createResouce() may be called by $this->autoAddIds()
            $query = RC::$pdo->prepare("DELETE FROM identifiers WHERE id = ?");
            $query->execute([$this->id]);
            foreach ($ids as $v) {
                RC::$log->debug("\tadding id " . $v);
                $queryI->execute([$this->id, $v]);
            }
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case Transaction::PG_DUPLICATE_KEY:
                    throw new DuplicatedKeyException('Duplicated resource identifier', 409, $e);
                case Transaction::PG_WRONG_DATE_VALUE:
                case Transaction::PG_WRONG_TEXT_VALUE:
                case Transaction::PG_WRONG_BINARY_VALUE:
                    throw new RepoException('Wrong property value', 400, $e);
                default:
                    throw $e;
            }
        }
    }

    /**
     * Updates system-managed metadata, e.g. who and when lastly modified a resource
     * @return void
     */
    private function manageSystemMetadata(DatasetNodeInterface $meta): void {
        $node   = $this->graph->getNode();
        $schema = RC::$schema;

        // delete properties scheduled for removal
        foreach ($meta->listObjects(new PT($schema->delete)) as $i) {
            $meta->delete(new PT($i));
        }
        $meta->delete(new PT($schema->delete));

        // repo-id
        $meta->add(DF::quad($node, $schema->id, $node));

        // creation date & user
        $date = (new DateTime())->format('Y-m-d\TH:i:s.u');
        if ($meta->none(new PT($schema->creationDate))) {
            $meta->add(DF::quad($node, $schema->creationDate, DF::literal($date, null, RDF::XSD_DATE_TIME)));
        }
        if ($meta->none(new PT($schema->creationUser))) {
            $meta->add(DF::quad($node, $schema->creationUser, DF::literal(RC::$auth->getUserName())));
        }
        // last modification date & user
        $meta->delete(new PT($schema->modificationDate));
        $meta->add(DF::quad($node, $schema->modificationDate, DF::literal($date, null, RDF::XSD_DATE_TIME)));
        $meta->delete(new PT($schema->modificationUser));
        $meta->add(DF::quad($node, $schema->modificationUser, DF::literal(RC::$auth->getUserName())));

        // check single id in the repo base url namespace which maches object's $id property
        $baseUrl    = RC::getBaseUrl();
        $baseUrlLen = strlen($baseUrl);
        foreach ($meta->listObjects(new PT($schema->id)) as $i) {
            if (!($i instanceof NamedNodeInterface)) {
                throw new RepoException('Non-resource identifier', 400);
            }
            $i = $i->getValue();
            if (str_starts_with($i, $baseUrl)) {
                $i = substr($i, $baseUrlLen);
                if ($i !== (string) $this->id) {
                    throw new RepoException("Id in the repository base URL namespace which does not match the resource id $i !== " . $this->id, 400);
                }
            }
        }
    }

    /**
     * @return void
     */
    public function setResponseBody(string $format): void {
        RC::setOutput(RdfUtil::serialize($this->graph->getDataset(), $format), $format);
    }

    private function autoAddId(string $ids): bool {
        $action = RC::$config->metadataManagment->autoAddIds->default;
        foreach (RC::$config->metadataManagment->autoAddIds->skipNamespaces as $i) {
            if (strpos($ids, $i) === 0) {
                $action = 'skip';
                break;
            }
        }
        foreach (RC::$config->metadataManagment->autoAddIds->addNamespaces as $i) {
            if (strpos($ids, $i) === 0) {
                $action = 'add';
                break;
            }
        }
        foreach (RC::$config->metadataManagment->autoAddIds->denyNamespaces as $i) {
            if (strpos($ids, $i) === 0) {
                $action = 'deny';
                break;
            }
        }
        switch ($action) {
            case 'deny':
                RC::$log->error("\t\tdenied to create resource $ids");
                throw new RepoException('Denied to create a non-existing id', 400);
            case 'add':
                RC::$log->info("\t\tadding resource $ids <--");
                try {
                    RC::$transaction->createResource(RC::$logId, [$ids]);
                } catch (DuplicatedKeyException $e) {
                    RC::$log->info("\t\t-->adding resource $ids (added by someone else)");
                    return true;
                }
                RC::$log->info("\t\t-->adding resource $ids");
                return true;
            default:
                RC::$log->info("\t\tskipped creation of resource $ids");
        }
        return false;
    }
}
