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
use RuntimeException;
use EasyRdf\Format;
use EasyRdf\Graph;
use EasyRdf\Resource;
use EasyRdf\Literal;
use zozlak\HttpAccept;
use zozlak\RdfConstants as RDF;
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

    /**
     * 
     * @param Resource $resource
     * @param string $property
     * @return array<string>
     */
    static public function propertyAsString(Resource $resource, string $property): array {
        $values = [];
        foreach ($resource->all($property) as $i) {
            $values[] = (string) $i;
        }
        return $values;
    }

    static public function idAsUri(int $id): string {
        return RC::getBaseUrl() . $id;
    }

    static public function getAcceptedFormats(): string {
        return Format::getHttpAcceptHeader();
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

    static public function outputHeaders(string $format = null): string {
        if (empty($format)) {
            $format = self::negotiateFormat();
        }
        header('Content-Type: ' . $format);
        return $format;
    }

    private int $id;
    private Graph $graph;

    public function __construct(?int $id = null) {
        if ($id !== null) {
            $this->id = $id;
        }
        $this->graph = new Graph();
    }

    public function setId(int $id): void {
        $oldMeta  = $this->getResource();
        $this->id = $id;
        if ($oldMeta !== null) {
            $meta        = $oldMeta->copy([], '/^$/', $this->getUri());
            $this->graph = $meta->getGraph();
        }
    }

    public function getUri(): string {
        return isset($this->id) ? self::idAsUri($this->id) : RC::getBaseUrl();
    }

    /**
     * 
     * @param \EasyRdf\Resource $newMeta
     * @param array<string> $preserve
     * @return void
     */
    public function update(Resource $newMeta, array $preserve = []): void {
        $this->graph->resource($this->getUri())->merge($newMeta, $preserve);
    }

    public function loadFromRequest(string $resUri = null): int {
        $body   = (string) file_get_contents('php://input');
        $format = filter_input(INPUT_SERVER, 'CONTENT_TYPE');
        if (empty($body) && empty($format)) {
            $format = 'application/n-triples';
        }
        $graph = new Graph();
        $count = $graph->parse($body, $format);

        if (empty($resUri)) {
            $resUri = $this->getUri();
        }
        if (count($graph->resource($resUri)->propertyUris()) === 0) {
            RC::$log->warning("No metadata for $resUri \n" . $graph->serialise('turtle'));
        }
        $graph->resource($resUri)->copy([], '/^$/', $this->getUri(), $this->graph);
        return $count;
    }

    public function loadFromResource(Resource $res): void {
        $this->graph = $res->getGraph();
    }

    public function loadFromDb(string $mode, ?string $property = null): void {
        $schema      = new Schema(RC::$config->schema);
        $headers     = new Schema(RC::$config->rest->headers);
        $nonRelProp  = RC::$config->metadataManagment->nonRelationProperties;
        $repo        = new RepoDb(RC::getBaseUrl(), $schema, $headers, RC::$pdo, $nonRelProp, RC::$auth);
        $res         = new RepoResourceDb((string) $this->id, $repo);
        $res->loadMetadata(true, $mode, $property);
        $this->graph = $res->getGraph()->getGraph();
    }

    public function getResource(): Resource {
        return $this->graph->resource($this->getUri());
    }

    public function merge(string $mode): Resource {
        $uri = $this->getUri();
        switch ($mode) {
            case self::SAVE_ADD:
                RC::$log->debug("\tadding metadata");
                $tmp  = new Metadata($this->id);
                $tmp->loadFromDb(RRI::META_RESOURCE);
                $meta = $tmp->graph->resource($uri);
                $new  = $this->graph->resource($uri);
                foreach ($new->propertyUris() as $p) {
                    foreach ($new->all($p) as $v) {
                        $meta->add($p, $v);
                    }
                }
                break;
            case self::SAVE_MERGE:
                RC::$log->debug("\tmerging metadata");
                $tmp  = new Metadata($this->id);
                $tmp->loadFromDb(RRI::META_RESOURCE);
                $meta = $tmp->graph->resource($uri);
                $meta->merge($this->graph->resource($uri), [RC::$config->schema->id]);
                break;
            case self::SAVE_OVERWRITE:
                RC::$log->debug("\toverwriting metadata");
                $meta = $this->graph->resource($uri);
                break;
            default:
                throw new RepoException('Wrong metadata merge mode ' . $mode, 400);
        }
        $this->manageSystemMetadata($meta);
        RC::$log->debug("\n" . $meta->getGraph()->serialise('turtle'));

        $this->graph = $meta->getGraph();
        return $meta;
    }

    public function save(): void {
        $spatialProps = RC::$config->spatialSearch->properties ?? [];
        $idProp       = RC::$config->schema->id;
        $query        = RC::$pdo->prepare("DELETE FROM metadata WHERE id = ?");
        $query->execute([$this->id]);
        $query        = RC::$pdo->prepare("DELETE FROM relations WHERE id = ?");
        $query->execute([$this->id]);

        $meta = $this->graph->resource($this->getUri());
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
            $ids        = self::propertyAsString($meta, $idProp);
            $properties = array_diff($meta->propertyUris(), [$idProp]);
            foreach ($properties as $p) {
                if (in_array($p, RC::$config->metadataManagment->nonRelationProperties)) {
                    $resources = [];
                    $literals  = $meta->all($p);
                } else {
                    $resources = $meta->allResources($p);
                    $literals  = $meta->allLiterals($p);
                }
                $spatial = in_array($p, $spatialProps);

                foreach ($resources as $v) {
                    $v = (string) $v;
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
                }

                foreach ($literals as $v) {
                    /* @var $v \EasyRdf\Literal */
                    $lang = '';
                    $type = is_a($v, '\EasyRdf\Resource') ? self::TYPE_URI : $v->getDatatypeUri();
                    $type = $spatial ? self::TYPE_GEOM : $type;
                    $vv   = (string) $v;
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
                        if (empty($type)) {
                            $type = RDF::XSD_STRING;
                        }
                        if ($type === RDF::XSD_STRING && $v instanceof \EasyRdf\Literal) {
                            $lang = $v->getLang() ?? '';
                        }
                        $param = [$this->id, $p, $type, $lang, null, null, $vv];
                    }
                    $queryV->execute($param);
                }
            }

            // Postpone processing ids because it would lock identifiers db table
            // and as a consequence prevent Transaction::createResource() from working
            // and Transaction::createResouce() may be called by $this->autoAddIds()
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
    private function manageSystemMetadata(Resource $meta): void {
        // delete properties scheduled for removal
        $delProp = RC::$config->schema->delete;
        foreach ($meta->all($delProp) as $i) {
            $meta->deleteResource((string) $i);
            $meta->delete((string) $i);
        }
        $meta->deleteResource($delProp);

        // repo-id
        $meta->addResource(RC::$config->schema->id, $this->getUri());

        $date = (new DateTime())->format('Y-m-d\TH:i:s.u');
        $type = 'http://www.w3.org/2001/XMLSchema#dateTime';
        // creation date & user
        if ($meta->getLiteral(RC::$config->schema->creationDate) === null) {
            $meta->addLiteral(RC::$config->schema->creationDate, new Literal($date, null, $type));
        }
        if ($meta->getLiteral(RC::$config->schema->creationUser) === null) {
            $meta->addLiteral(RC::$config->schema->creationUser, RC::$auth->getUserName());
        }
        // last modification date & user
        $meta->delete(RC::$config->schema->modificationDate);
        $meta->addLiteral(RC::$config->schema->modificationDate, new Literal($date, null, $type));
        $meta->delete(RC::$config->schema->modificationUser);
        $meta->addLiteral(RC::$config->schema->modificationUser, RC::$auth->getUserName());

        // check single id in the repo base url namespace which maches object's $id property
        foreach ($meta->all(RC::$config->schema->id) as $i) {
            if (!is_a($i, Resource::class)) {
                throw new RepoException('Non-resource identifier', 400);
            }
            $i = (string) $i;
            if (strpos($i, RC::getBaseUrl()) === 0) {
                $i = substr($i, strlen(RC::getBaseUrl()));
                if ($i !== (string) $this->id) {
                    throw new RepoException("Id in the repository base URL namespace which does not match the resource id $i !== " . $this->id, 400);
                }
            }
        }
    }

    /**
     * @return void
     */
    public function outputRdf(string $format): void {
        echo $this->graph->serialise($format);
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
