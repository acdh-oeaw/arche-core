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

use EasyRdf\Graph;
use EasyRdf\Resource;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use acdhOeaw\acdhRepo\RestController as RC;

/**
 * Represents a request binary payload.
 *
 * @author zozlak
 */
class BinaryPayload {

    const FTS_PROPERTY = 'BINARY';

    /**
     *
     * @var int
     */
    private $id;
    private $hash;
    private $size;
    private $keepAliveHandle;
    private $keepAliveTimeout;

    public function __construct(int $id) {
        $this->id = $id;
    }

    public function setKeepAliveHandle(callable $handle, int $timeout = 1) {
        $this->keepAliveHandle  = $handle;
        $this->keepAliveTimeout = $timeout;
    }

    public function upload(): void {
        $tmpPath    = RC::$config->storage->tmpDir . '/' . $this->id;
        $input      = fopen('php://input', 'rb');
        $output     = fopen($tmpPath, 'wb');
        $this->size = 0;
        $hash       = hash_init(RC::$config->storage->hashAlgorithm);
        $time       = time();
        while (!feof($input)) {
            $buffer     = fread($input, 1048576);
            hash_update($hash, $buffer);
            $this->size += fwrite($output, $buffer);

            $curTime = time();
            if ($this->keepAliveHandle !== null && $curTime - $time >= $this->keepAliveTimeout) {
                $handle = $this->keepAliveHandle;
                $handle();
                $time   = $curTime;
                RC::$log->debug("\tprolonging transaction");
            }
        }
        fclose($input);
        fclose($output);
        $this->hash = RC::$config->storage->hashAlgorithm . ':' . hash_final($hash, false);

        $digest = filter_input(INPUT_SERVER, 'HTTP_DIGEST'); // client-side hash to be compared after the upload
        if (!empty($digest)) {
            //TODO - see https://fedora.info/2018/11/22/spec/#http-post
        }

        $targetPath = $this->getPath(true);
        rename($tmpPath, $targetPath);
        if ($this->size === 0) {
            $this->hash = null;
            unlink($targetPath);
        }

        // full text search
        $c         = RC::$config->fullTextSearch;
        $tikaFlag  = !empty($c->tikaLocation);
        $sizeFlag  = $this->size <= $this->toBytes($c->sizeLimits->indexing);
        list($mimeType, $fileName) = $this->getRequestMetadataRaw();
        $mimeMatch = in_array($mimeType, $c->mimeFilter->mime);
        $mimeType  = $c->mimeFilter->type;
        $mimeFlag  = $mimeType === Metadata::FILTER_SKIP && !$mimeMatch || $mimeType === Metadata::FILTER_INCLUDE && $mimeMatch;
        if ($tikaFlag && $sizeFlag && $mimeFlag) {
            $result = $this->updateFts();
            RC::$log->debug("\tupdating full text search: " . (int) $result);
        } else {
            RC::$log->debug("\tskipping full text search update ($tikaFlag, $sizeFlag, $mimeFlag)");
        }
    }

    public function outputHeaders(): void {
        $query = RC::$pdo->prepare("
            SELECT *
            FROM
                          (SELECT id, value   AS filename FROM metadata WHERE property = ? AND id = ? LIMIT 1) t1
                FULL JOIN (SELECT id, value   AS mime     FROM metadata WHERE property = ? AND id = ? LIMIT 1) t2 USING (id)
                FULL JOIN (SELECT id, value_n AS size     FROM metadata WHERE property = ? AND id = ? LIMIT 1) t3 USING (id)
        ");
        $query->execute([
            RC::$config->schema->fileName, $this->id,
            RC::$config->schema->mime, $this->id,
            RC::$config->schema->binarySize, $this->id
        ]);
        $data  = $query->fetchObject();
        if ($data === false) {
            $data = ['filename' => '', 'mime' => '', 'size' => ''];
        }
        if (!empty($data->filename)) {
            header('Content-Disposition: attachment; filename="' . $data->filename . '"');
        }
        if (!empty($data->mime)) {
            header('Content-Type: ' . $data->mime);
        }
        if (!empty($data->size)) {
            header('Content-Length: ' . $data->size);
        }
    }

    public function getRequestMetadata(): Resource {
        list($contentType, $fileName) = $this->getRequestMetadataRaw();

        $graph = new Graph();
        $meta  = $graph->newBNode();
        $meta->addLiteral(RC::$config->schema->mime, $contentType);
        if (!empty($fileName)) {
            $meta->addLiteral(RC::$config->schema->fileName, $fileName);
        } else {
            $meta->addResource(RC::$config->schema->delete, RC::$config->schema->fileName);
        }
        if ($this->size > 0) {
            $meta->addLiteral(RC::$config->schema->binarySize, $this->size);
        } else {
            $meta->addResource(RC::$config->schema->delete, RC::$config->schema->binarySize);
        }
        if ($this->size > 0) {
            $meta->addLiteral(RC::$config->schema->hash, $this->hash);
        } else {
            $meta->addResource(RC::$config->schema->delete, RC::$config->schema->hash);
        }
        return $meta;
    }

    public function backup(string $suffix): bool {
        $srcPath = $this->getPath(false);
        return file_exists($srcPath) && rename($srcPath, $this->getPath(true, $suffix));
    }

    public function restore(string $suffix): bool {
        $backupPath = $this->getPath(false, $suffix);
        if (file_exists($backupPath)) {
            return rename($backupPath, $this->getPath(true));
        }
        return false;
    }

    public function delete(string $suffix = ''): bool {
        $path = $this->getPath(false, $suffix);
        if (file_exists($path)) {
            return unlink($path);
        }
        return false;
    }

    public function getPath(bool $create = false, string $suffix = ''): string {
        return $this->getStorageDir($this->id, $create) . '/' . $this->id . (empty($suffix) ? '' : '.' . $suffix);
    }

    private function getStorageDir(int $id, bool $create, string $path = null,
                                   int $level = 0): string {
        if (empty($path)) {
            $path = RC::$config->storage->dir;
        }
        if ($level < RC::$config->storage->levels) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            if ($create && !file_exists($path)) {
                mkdir($path, base_convert(RC::$config->storage->modeDir, 8, 10));
            }
            $path = $this->getStorageDir((int) $id / 100, $create, $path, $level + 1);
        }
        return $path;
    }

    private function getRequestMetadataRaw(): array {
        $contentDisposition = trim(filter_input(INPUT_SERVER, 'HTTP_CONTENT_DISPOSITION'));
        $fileName           = null;
        if (preg_match('/^attachment; filename=/', $contentDisposition)) {
            $fileName = preg_replace('/^attachment; filename="?/', '', $contentDisposition);
            $fileName = preg_replace('/"$/', '', $fileName);
        }

        $contentType = filter_input(INPUT_SERVER, 'CONTENT_TYPE');
        if (empty($contentType)) {
            if (!empty($fileName)) {
                $contentType = \GuzzleHttp\Psr7\mimetype_from_filename($fileName);
                if ($contentType === null) {
                    $contentType = mime_content_type($this->getPath(false));
                }
            }
            if (empty($contentType)) {
                $contentType = RC::$config->rest->defaultMime;
            }
        }

        return [$contentType, $fileName];
    }

    private function updateFts(): bool {
        $limit  = $this->toBytes(RC::$config->fullTextSearch->sizeLimits->highlighting);
        $result = false;
        $query  = RC::$pdo->prepare("DELETE FROM full_text_search WHERE id = ? AND property = ?");
        $query->execute([$this->id, self::FTS_PROPERTY]);

        $query = RC::$pdo->prepare("INSERT INTO full_text_search (id, property, segments, raw) VALUES (?, ?, to_tsvector('simple', ?), ?)");
        $tika  = RC::$config->fullTextSearch->tikaLocation;
        if (substr($tika, 0, 4) === 'http') {
            $client = new Client(['http_errors' => false]);
            $input  = fopen($this->getPath(false), 'r');
            $req    = new Request('put', $tika . 'tika', ['Accept' => 'text/plain'], $input);
            $resp   = $client->send($req);
            if ($resp->getStatusCode() === 200) {
                $body   = (string) $resp->getBody();
                $query->execute([$this->id, self::FTS_PROPERTY, $body, strlen($body) <= $limit ? $body : null]);
                $result = true;
            }
        } else {
            $output = $ret    = '';
            exec($tika . ' ' . escapeshellarg($this->getPath(false)), $output, $ret);
            $output = implode($output);
            if ($ret === 0) {
                $query->execute([$this->id, self::FTS_PROPERTY, $output, strlen($output) <= $limit ? $output : null]);
                $result = true;
            }
        }
        return $result;
    }

    private function toBytes(string $number): int {
        $number = strtolower($number);
        $from   = ['k', 'm', 'g', 't'];
        $to     = ['000', '0000000', '0000000000', '0000000000000'];
        $number = str_replace($from, $to, $number);
        return (int) $number;
    }

}