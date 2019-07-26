<?php

/*
 * The MIT License
 *
 * Copyright 2019 zozlak.
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

use PDO;
use PDOException;
use EasyRdf\Graph;
use EasyRdf\Literal;

/**
 * Description of RestController
 *
 * @author zozlak
 */
class RestController {

    static private $outputFormats = [
        'text/turtle'           => 'turtle',
        'application/rdf+xml'   => 'rdfxml',
        'application/n-triples' => 'ntriples',
        'application/ld+json'   => 'jsonld',
        '*/*'                   => 'turtle',
        'text/*'                => 'turtle',
        'application/*'         => 'ntriples',
    ];

    /**
     *
     * @var object
     */
    private $config;
    private $pdo;
    private $routes = [
        'POST'   => [
            '|^sparql/?$|'                                        => 'dummySparql',
            '|^fcr:tx/?$|'                                        => 'beginTransaction',
            '&^tx:[0-9]+/fcr:tx/?((fcr:commit|fcr:rollback)/?)?&' => 'dummyTransaction',
            '//'                                                  => 'createResource',
        ],
        'PATCH'  => [
            '|/fcr:metadata/?$|' => 'patchResourceMetadata',
        ],
        'PUT'    => [
            '//' => 'putResource',
        ],
        'DELETE' => [
            '//' => 'deleteResource',
        ],
        'GET'    => [
            '|/fcr:metadata/?$|' => 'getResourceMetadata',
            '//'                 => 'getResource'
        ]
    ];

    public function __construct(object $config) {
        $this->config = $config;
        $this->pdo    = new PDO($this->config->dbConnStr);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->beginTransaction();
    }

    public function __destruct() {
        $this->pdo->commit();
    }

    public function handleRequest(): void {
        $method = filter_input(INPUT_SERVER, 'REQUEST_METHOD');
        if (!isset($this->routes[$method])) {
            http_response_code(501);
        } else {
            $path    = substr(filter_input(INPUT_SERVER, 'REQUEST_URI'), strlen($this->config->rest->pathBase));
            $handled = false;
            foreach ($this->routes[$method] as $regex => $method) {
                if (preg_match($regex, $path)) {
                    $handled = true;
                    $this->$method();
                    break;
                }
            }
            if (!$handled) {
                http_response_code(404);
            }
        }
    }

    private function deleteResource(): void {
        list($id, $tx) = $this->getId();

        $query = $this->pdo->prepare('DELETE FROM resources WHERE id = ?');
        try {
            $query->execute([$id]);
            http_response_code(204);
        } catch (PDOException $ex) {
            http_response_code(409);
        }
    }

    private function getResource(): void {
        list($id, $tx) = $this->getId();
        $path = $this->getResourcePath($id, false);
        if (file_exists($path)) {
            readfile($path);
        } else {
            http_response_code(204);
        }
    }

    private function patchResourceMetadata(): void {
        list($id, $tx) = $this->getId();

        $contentType = filter_input(INPUT_SERVER, 'CONTENT_TYPE');
        if ($contentType === 'application/sparql-update') {
            // https://github.com/farafiri/PHP-parsing-tool

            $input = fopen('php://input', 'r');
            $raw   = str_replace(["\n", "\r"], '', stream_get_contents($input));
            fclose($input);

            $delete = preg_replace('/.*(DELETE *{[^}]*}).*/', '\\1', $raw);
            preg_match_all('/<([^>]+)> *<([^>]+)> *("[^"]+"|<[^>]+>)/', $delete, $delete, PREG_SET_ORDER);
            $queryM = $this->pdo->prepare("DELETE FROM metadata WHERE (id, property, coalesce(value, textraw)) = (?, ?, ?)");
            $queryI = $this->pdo->prepare("DELETE FROM identifiers WHERE (id, ids) = (?, ?)");
            $queryR = $this->pdo->prepare("DELETE FROM relations WHERE (id, property) = (?, ?) AND target_id = (SELECT id FROM identifiers WHERE ids = ?)");
            foreach ($delete as $i) {
                if (substr($i[3], 0, 1) === '"') {
                    $queryM->execute([$id, $i[2], substr($i[3], 1, -1)]);
                } else if ($i[2] == $this->config->schema->id) {
                    $queryI->execute([$id, substr($i[3], 1, -1)]);
                    echo "removing id " . substr($i[3], 1, -1) . "\n";
                } else {
                    $queryR->execute([$id, $i[2], substr($i[3], 1, -1)]);
                }
            }

            $insert = preg_replace('/.*(INSERT *{[^}]*}).*/', '\\1', $raw);
            preg_match_all('/<([^>]+)> *<([^>]+)> *("[^"]+"|<[^>]+>)/', $insert, $insert, PREG_SET_ORDER);
            $queryV = $this->pdo->prepare("INSERT INTO metadata (id, property, type, lang, value_n, value_t, value) VALUES (?, ?, ?, '', ?, ?, ?)");
            $queryS = $this->pdo->prepare("INSERT INTO metadata (id, property, type, lang, text, textraw) VALUES (?, ?, 'http://www.w3.org/2001/XMLSchema#string', '', to_tsvector(?), ?)");
            $queryI = $this->pdo->prepare("INSERT INTO identifiers (id, ids) VALUES (?, ?)");
            $queryR = $this->pdo->prepare("INSERT INTO relations (id, target_id, property) SELECT ?, id, ? FROM identifiers WHERE ids = ?");
            foreach ($insert as $i) {
                if (substr($i[3], 0, 1) === '"') {
                    $value = substr($i[3], 1, -1);
                    if (is_numeric($value)) {
                        $queryV->execute([$id, $i[2], 'http://www.w3.org/2001/XMLSchema#long',
                            $value, null, $value]);
                    } else if (preg_match('/^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T[0-9][0-9](:[0-9][0-9])?(:[0-9][0-9])?([.][0-9]+)?Z?)?$/', $value)) {
                        $queryV->execute([$id, $i[2], 'http://www.w3.org/2001/XMLSchema#dateTime',
                            null, $value, $value]);
                    } else {
                        $queryS->execute([$id, $i[2], $value, $value]);
                    }
                } else if ($i[2] == $this->config->schema->id) {
                    echo "adding id " . substr($i[3], 1, -1) . "\n";
                    $queryI->execute([$id, substr($i[3], 1, -1)]);
                } else {
                    $queryR->execute([$id, substr($i[3], 1, -1), $i[2]]);
                }
            }
            $this->getResourceMetadata();
        } else if (isset(self::$outputFormats[$contentType])) {
            http_response_code(501);
        } else {
            http_response_code(415);
        }
    }

    private function copyResource(int $id): array {
        $tmpPath = $this->config->storage->tmpDir . '/' . $id;
        $input   = fopen('php://input', 'rb');
        $output  = fopen($tmpPath, 'wb');
        $size    = 0;
        $hash    = hash_init($this->config->storage->hashAlgorithm);
        while (!feof($input)) {
            $buffer = fread($input, 1048576);
            hash_update($hash, $buffer);
            $size   += fwrite($output, $buffer);
        }
        fclose($input);
        fclose($output);
        $hash = $this->config->storage->hashAlgorithm . ':' . hash_final($hash, false);

        $digest = filter_input(INPUT_SERVER, 'HTTP_DIGEST'); // client-side hash to be compared after the upload
        if (!empty($digest)) {
            //TODO - see https://fedora.info/2018/11/22/spec/#http-post
        }

        $contentType        = filter_input(INPUT_SERVER, 'CONTENT_TYPE') ?? 'application/octet-stream';
        $contentDisposition = trim(filter_input(INPUT_SERVER, 'HTTP_CONTENT_DISPOSITION'));

        $fileName = null;
        if (preg_match('/^attachment; filename=/', $contentDisposition)) {
            $fileName = preg_replace('/^attachment; filename="?/', '', $contentDisposition);
            $fileName = preg_replace('/"$/', '', $fileName);
        }

        $queryD = $this->pdo->prepare("DELETE FROM metadata WHERE (id, property) = (?, ?)");
        $queryV = $this->pdo->prepare("INSERT INTO metadata (id, property, type, lang, value_n, value_t, value) VALUES (?, ?, ?, '', ?, ?, ?)");
        $queryS = $this->pdo->prepare("INSERT INTO metadata (id, property, type, lang, text, textraw) VALUES (?, ?, 'http://www.w3.org/2001/XMLSchema#string', '', to_tsvector(?), ?)");

        foreach ($this->config->schema->fileName as $i) {
            $queryD->execute([$id, $i]);
            $queryS->execute([$id, $i, $fileName, $fileName]);
        }
        foreach ($this->config->schema->mime as $i) {
            $queryD->execute([$id, $i]);
            $queryS->execute([$id, $i, $contentType, $contentType]);
        }
        foreach ($this->config->schema->binarySize as $i) {
            $queryD->execute([$id, $i]);
            $queryV->execute([$id, $i, 'http://www.w3.org/2001/XMLSchema#long', $size,
                null, $size]);
        }
        foreach ($this->config->schema->hash as $i) {
            $queryD->execute([$id, $i]);
            $queryS->execute([$id, $i, $hash, $hash]);
        }

        return [$tmpPath, $size, $hash];
    }

    private function putResource(): void {
        list($id, $tx) = $this->getId();

        list($tmpPath, $size, $hash) = $this->copyResource($id);

        $path = $this->getResourcePath($id, true);
        rename($tmpPath, $path);
        http_response_code(204);
    }

    private function createResource(): void {
        $query = $this->pdo->query("INSERT INTO resources (id) VALUES (nextval('id_seq')) RETURNING id");
        $id    = $query->fetchColumn();

        $query = $this->pdo->prepare("INSERT INTO identifiers (ids, id) VALUES (?, ?)");
        $query->execute([$this->getUriBase() . $id, $id]);

        list($tmpPath, $size, $hash) = $this->copyResource($id);

        $path = $this->getResourcePath($id, true);
        rename($tmpPath, $path);
        http_response_code(201);
        header('Location: ' . $this->getUriBase() . $id);
    }

    private function getResourcePath(int $id, bool $create): string {
        return $this->getStorageDir($id, $create) . '/' . $id;
    }

    private function getStorageDir(int $id, bool $create, string $path = null,
                                   int $level = 0): string {
        if (empty($path)) {
            $path = $this->config->storage->dir;
        }
        if ($level < $this->config->storage->levels) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            if ($create && !file_exists($path)) {
                mkdir($path, base_convert($this->config->storage->modeDir, 8, 10));
            }
            $path = $this->getStorageDir((int) $id / 100, $create, $path, $level + 1);
        }
        return $path;
    }

    private function getUriBase(): string {
        return $this->config->rest->urlBase . $this->config->rest->pathBase;
    }

    private function beginTransaction(): void {
        http_response_code(201);
        header('Location: ' . $this->getUriBase() . 'tx:0');
    }

    private function dummyTransaction(): void {
        http_response_code(204);
    }

    private function getId(): array {
        $id = substr(filter_input(INPUT_SERVER, 'REQUEST_URI'), strlen($this->config->rest->pathBase));
        $id = preg_replace('|/fcr:metadata/?$|', '', $id);
        $tx = preg_replace('|^(tx:[0-9]+/).*|', '\\1', $id);
        $tx = $tx !== $id ? $tx : '';
        $id = (int) preg_replace('|^tx:[0-9]+/|', '', $id);
        return [$id, $tx];
    }

    private function getResourceMetadata(): void {
        list($id, $tx) = $this->getId();
        if ($id === 0) {
            http_response_code(404);
        } else {
            $graph  = new Graph();
            $query  = $this->pdo->prepare("
            SELECT * FROM get_neighbors_metadata(?, ?)
        ");
            $query->execute([$id, $this->config->schema->parent]);
            while ($triple = $query->fetchObject()) {
                $idTmp    = $this->getUriBase() . ($triple->id === $id ? $tx : '') . $triple->id;
                $resource = $graph->resource($idTmp);
                switch ($triple->type) {
                    case 'ID':
                        $resource->addResource($this->config->schema->id, $triple->value);
                        break;
                    case 'URI':
                        $resource->addResource($triple->property, $this->getUriBase() . $triple->value);
                        break;
                    default:
                        $literal = new Literal($triple->value, !empty($triple->lang) ? $triple->lang : null, $triple->type);
                        $resource->add($triple->property, $literal);
                }
            }
            foreach ($this->config->rest->fixedMetadata as $property => $values) {
                foreach ($values as $value) {
                    $resource->addResource($property, $value);
                }
            }
            header('Content-Type: application/n-triples');

            $accept = filter_input(INPUT_SERVER, 'HTTP_ACCEPT') ?? '*/*';
            echo $graph->serialise(self::$outputFormats[$accept]);
        }
    }

    private function dummySparql(): void {
        $queryStr = str_replace(["\n", "\r"], '', filter_input(INPUT_POST, 'query'));
        $ids      = preg_replace('|^.*<' . $this->config->schema->id . '> <([^>]+)>.*$|', '\\1', $queryStr);
        $query    = $this->pdo->prepare("SELECT id FROM identifiers WHERE ids = ?");
        $query->execute([$ids]);
        $id       = $query->fetchColumn();

        $resTmpl = [
            'head'    => ['vars' => ['res']],
            'results' => ['bindings' => []]
        ];
        if ($id !== false) {
            $resTmpl['results']['bindings'][] = [
                'res' => [
                    'type'  => 'uri',
                    'value' => $this->getUriBase() . $id
                ]
            ];
        }
        header('Content-Type: application/sparql-results+json');
        echo json_encode($resTmpl);
    }

}