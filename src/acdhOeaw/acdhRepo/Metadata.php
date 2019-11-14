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

namespace acdhOeaw\acdhRepo;

use BadMethodCallException;
use DateTime;
use PDOStatement;
use PDOException;
use RuntimeException;
use EasyRdf\Format;
use EasyRdf\Graph;
use EasyRdf\Resource;
use EasyRdf\Literal;
use zozlak\HttpAccept;
use acdhOeaw\acdhRepo\RestController as RC;

/**
 * Manages resources's metadata (loads from database or HTTP request, writes into
 * the database, serializes to RDF, etc.).
 *
 * @author zozlak
 */
class Metadata {

    const DATETIME_REGEX = '/^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T[0-9][0-9](:[0-9][0-9])?(:[0-9][0-9])?([.][0-9]+)?Z?)?$/';
    const LOAD_RESOURCE  = 'resource';
    const LOAD_NEIGHBORS = 'neighbors';
    const LOAD_RELATIVES = 'relatives';
    const SAVE_ADD       = 'add';
    const SAVE_OVERWRITE = 'overwrite';
    const SAVE_MERGE     = 'merge';
    const FILTER_SKIP    = 'skip';
    const FILTER_INCLUDE = 'include';

    static public function getAcceptedFormats(): string {
        return Format::getHttpAcceptHeader();
    }

    /**
     *
     * @var int
     */
    private $id;

    /**
     *
     * @var \EasyRdf\Graph
     */
    private $graph;

    public function __construct(int $id = null) {
        $this->id    = $id;
        $this->graph = new Graph();
    }

    public function getUri(): string {
        return RC::getBaseUrl() . $this->id;
    }

    public function update(Resource $newMeta, array $preserve = []): void {
        $this->graph->resource($this->getUri())->merge($newMeta, $preserve);
    }

    public function loadFromRequest(string $resUri = null): int {
        $body   = file_get_contents('php://input');
        $format = filter_input(INPUT_SERVER, 'HTTP_CONTENT_TYPE');
        if (empty($body) && empty($format)) {
            $format = 'application/n-triples';
        }
        $graph = new Graph();
        $count = $graph->parse($body, $format);

        if (empty($resUri)) {
            $resUri = $this->getUri();
        }
        if (count($graph->resource($resUri)->propertyUris()) === 0) {
            RC::$log->warning("No metadata for for $resUri \n" . $graph->serialise('turtle'));
        }
        $graph->resource($resUri)->copy([], '/^$/', $this->getUri(), $this->graph);
        return $count;
    }

    public function loadFromResource(Resource $res): void {
        $this->graph = $res->getGraph();
    }

    public function loadFromDbQuery(PDOStatement $query): void {
        $this->graph = new Graph();
        $baseUrl     = RC::getBaseUrl();
        while ($triple      = $query->fetchObject()) {
            $triple->id = $baseUrl . $triple->id;
            $resource   = $this->graph->resource($triple->id);
            switch ($triple->type) {
                case 'ID':
                    $resource->addResource(RC::$config->schema->id, $triple->value);
                    break;
                case 'REL':
                    $resource->addResource($triple->property, $baseUrl . $triple->value);
                    break;
                case 'URI':
                    $resource->addResource($triple->property, $triple->value);
                    break;
                default:
                    $literal = new Literal($triple->value, !empty($triple->lang) ? $triple->lang : null, empty($triple->lang) ? $triple->type : null);
                    $resource->add($triple->property, $literal);
            }
        }
    }

    public function loadFromDb(string $mode, ?string $property = null): void {

        switch ($mode) {
            case self::LOAD_RESOURCE:
                $query = "SELECT * FROM (SELECT * FROM metadata_view WHERE id = ?) mt";
                $param = [$this->id];
                break;
            case self::LOAD_NEIGHBORS:
                $query = "SELECT * FROM get_neighbors_metadata(?, ?)";
                $param = [$this->id, $property];
                break;
            case self::LOAD_RELATIVES:
                $query = "SELECT * FROM get_relatives_metadata(?, ?)";
                $param = [$this->id, $property];
                break;
            default:
                throw new RepoException('Bad metadata mode ' . $mode, 400);
        }
        list($authQuery, $authParam) = RC::$auth->getMetadataAuthQuery();
        $query = RC::$pdo->prepare($query . $authQuery);
        $query->execute(array_merge($param, $authParam));
        $this->loadFromDbQuery($query);
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
                $tmp->loadFromDb(self::LOAD_RESOURCE);
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
                $tmp->loadFromDb(self::LOAD_RESOURCE);
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
        $query = RC::$pdo->prepare("DELETE FROM metadata WHERE id = ?");
        $query->execute([$this->id]);
        $query = RC::$pdo->prepare("DELETE FROM relations WHERE id = ?");
        $query->execute([$this->id]);
        $query = RC::$pdo->prepare("DELETE FROM identifiers WHERE id = ?");
        $query->execute([$this->id]);
        $query = RC::$pdo->prepare("DELETE FROM full_text_search WHERE id = ? AND property <> ?");
        $query->execute([$this->id, BinaryPayload::FTS_PROPERTY]);

        $meta = $this->graph->resource($this->getUri());
        try {
            $queryV = RC::$pdo->prepare("INSERT INTO metadata (id, property, type, lang, value_n, value_t, value) VALUES (?, ?, ?, ?, ?, ?, ?)");
            $queryF = RC::$pdo->prepare("INSERT INTO full_text_search (id, property, segments, raw) VALUES (?, ?, to_tsvector('simple', ?), ?)");
            $queryI = RC::$pdo->prepare("INSERT INTO identifiers (id, ids) VALUES (?, ?)");
            $queryR = RC::$pdo->prepare("INSERT INTO relations (id, target_id, property) SELECT ?, id, ? FROM identifiers WHERE ids = ?");
            foreach ($meta->propertyUris() as $p) {
                if ($p === RC::$config->schema->id) {
                    foreach ($meta->all($p) as $v) {
                        $v = (string) $v;
                        RC::$log->debug("\tadding id " . $v);
                        $queryI->execute([$this->id, $v]);
                    }
                } else {
                    $ftsMatch = in_array($p, RC::$config->fullTextSearch->propertyFilter->properties);
                    $ftsType  = RC::$config->fullTextSearch->propertyFilter->type;
                    $ftsFlag  = $ftsType === self::FILTER_SKIP && !$ftsMatch || $ftsType === self::FILTER_INCLUDE && $ftsMatch;

                    if (in_array($p, RC::$config->metadataManagment->nonRelationProperties)) {
                        $resources = [];
                        $literals  = $meta->all($p);
                    } else {
                        $resources = $meta->allResources($p);
                        $literals  = $meta->allLiterals($p);
                    }

                    foreach ($resources as $v) {
                        $v = (string) $v;
                        RC::$log->debug("\tadding relation " . $p . " " . $v);
                        $queryR->execute([$this->id, $p, $v]);
                        if ($queryR->rowCount() === 0) {
                            $added = $this->autoAddId($v);
                            if ($added) {
                                $queryR->execute([$this->id, $p, $v]);
                            }
                        }
                    }

                    foreach ($literals as $v) {
                        $lang = '';
                        $type = 'http://www.w3.org/2001/XMLSchema#string';
                        $vv   = (string) $v;
                        if (is_numeric($vv)) {
                            $type = 'http://www.w3.org/2001/XMLSchema#decimal';
                            $queryV->execute([$this->id, $p, $type, '', $vv, null,
                                $vv]);
                        } else if (preg_match(self::DATETIME_REGEX, $vv)) {
                            $type = 'http://www.w3.org/2001/XMLSchema#dateTime';
                            $queryV->execute([$this->id, $p, $type, '', null, $vv,
                                $vv]);
                        } else {
                            if (is_a($v, '\EasyRdf\Resource')) {
                                $type = 'URI';
                            } else {
                                $type = $v->getDatatypeUri() ?? 'http://www.w3.org/2001/XMLSchema#string';
                                $lang = $v->getLang() ?? '';
                            }
                            $queryV->execute([$this->id, $p, $type, $lang, null,
                                null,
                                $vv]);
                        }
                        if ($ftsFlag) {
                            $queryF->execute([$this->id, $p, $vv, $vv]);
                        }
                    }
                }
            }
        } catch (PDOException $e) {
            if ($e->getCode() == 23505) {
                throw new RepoException('Duplicated resource identifier', 400, $e);
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

        // Last modification date & user
        $date = (new DateTime())->format('Y-m-d\Th:i:s');
        $type = 'http://www.w3.org/2001/XMLSchema#dateTime';
        $meta->addLiteral(RC::$config->schema->modificationDate, new Literal($date, null, $type));
        $meta->addLiteral(RC::$config->schema->modificationUser, RC::$auth->getUserName());
    }

    public function outputHeaders(): string {
        $format = $this->negotiateFormat();
        header('Content-Type: ' . $format);
        return $format;
    }

    /**
     * @return void
     */
    public function outputRdf(string $format): void {
        echo $this->graph->serialise($format);
    }

    private function negotiateFormat(): string {
        try {
            $format = HttpAccept::getBestMatch(RC::$config->rest->metadataFormats)->getFullType();
        } catch (RuntimeException $e) {
            $format = RC::$config->rest->defaultMetadataFormat;
        }
        return $format;
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
                RC::$log->error("\t\tdenied to create resource " . $ids);
                throw new RepoException('Denied to create a non-existing id', 400);
            case 'add':
                RC::$log->info("\t\tadding resource " . $ids);
                $id   = RC::$pdo->query("INSERT INTO resources (id) VALUES (nextval('id_seq'::regclass)) RETURNING id")->fetchColumn();
                $meta = new Metadata($id);
                $meta->graph->resource($meta->getUri())->addResource(RC::$config->schema->id, $ids);
                $meta->update(RC::$auth->getCreateRights());
                $meta->merge(self::SAVE_OVERWRITE);
                $meta->save();
                return true;
            default:
                RC::$log->info("\t\tskipped creation of resource " . $ids);
        }
        return false;
    }

}
