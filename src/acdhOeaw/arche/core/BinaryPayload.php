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
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\util\SpatialInterface;
use acdhOeaw\arche\lib\BinaryPayload as BP;

/**
 * Represents a request binary payload.
 *
 * @author zozlak
 */
class BinaryPayload {

    static public function getStorageDir(int $id, string $path, int $level,
                                         int $levelMax): string {
        if ($level < $levelMax) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            $path = self::getStorageDir((int) ($id / 100), $path, $level + 1, $levelMax);
        }
        return $path;
    }

    private int $id;
    private ?string $hash;
    private int $size;
    private int $imagePxHeight;
    private int $imagePxWidth;
    private string $tmpPath;

    public function __construct(int $id) {
        $this->id = $id;
    }

    public function __destruct() {
        if (!empty($this->tmpPath) && file_exists($this->tmpPath)) {
            unlink($this->tmpPath);
        }
    }

    public function upload(): void {
        $this->tmpPath = RC::$config->storage->tmpDir . '/' . $this->id;
        $input         = fopen('php://input', 'rb') ?: throw new RepoException("Failed to open request body as a file");
        $output        = fopen($this->tmpPath, 'wb') ?: throw new RepoException("Failed to open local temporary storage");
        $this->size    = 0;
        $hash          = hash_init(RC::$config->storage->hashAlgorithm);
        while (!feof($input)) {
            $buffer     = (string) fread($input, 1048576);
            hash_update($hash, $buffer);
            $this->size += fwrite($output, $buffer);
        }
        fclose($input);
        fclose($output);
        $this->hash = RC::$config->storage->hashAlgorithm . ':' . hash_final($hash, false);

        $digest = filter_input(INPUT_SERVER, 'HTTP_DIGEST'); // client-side hash to be compared after the upload
        if (!empty($digest)) {
            //TODO - see https://fedora.info/2018/11/22/spec/#http-post
        }

        list($mimeType, $fileName) = $this->getRequestMetadataRaw();
        // full text search
        $query      = RC::$pdo->prepare("DELETE FROM full_text_search WHERE id = ?");
        $query->execute([$this->id]);
        $c          = RC::$config->fullTextSearch;
        $tikaFlag   = !empty($c->tikaLocation);
        $sizeFlag   = $this->size <= $this->toBytes($c->sizeLimits->indexing) && $this->size > 0;
        $mimeMatch  = in_array($mimeType, $c->mimeFilter->mime);
        $filterType = $c->mimeFilter->type;
        $mimeFlag   = $filterType === Metadata::FILTER_SKIP && !$mimeMatch || $filterType === Metadata::FILTER_INCLUDE && $mimeMatch;
        if ($tikaFlag && $sizeFlag && $mimeFlag) {
            RC::$log->debug("\tupdating full text search (tika: $tikaFlag, size: $sizeFlag, mime: $mimeFlag, mime type: $mimeType)");
            $result = $this->updateFts();
            RC::$log->debug("\t\tresult: " . (int) $result);
        } else {
            RC::$log->debug("\tskipping full text search update (tika: $tikaFlag, size: $sizeFlag, mime: $mimeFlag, mime type: $mimeType)");
        }
        // spatial search
        $query    = RC::$pdo->prepare("DELETE FROM spatial_search WHERE id = ?");
        $query->execute([$this->id]);
        $c        = RC::$config->spatialSearch;
        $mimeFlag = isset($c->mimeTypes->$mimeType);
        $sizeFlag = $this->size <= $this->toBytes($c->sizeLimit) && $this->size > 0;
        if ($mimeFlag && $sizeFlag) {
            RC::$log->debug("\tupdating spatial search (size: $sizeFlag, mime: $mimeFlag, mime type: $mimeType)");
            $this->updateSpatialSearch(call_user_func($c->mimeTypes->$mimeType));
        } else {
            RC::$log->debug("\tskipping spatial search (size: $sizeFlag, mime: $mimeFlag, mime type: $mimeType)");
        }
        // image dimensions
        if (str_starts_with($mimeType, 'image/')) {
            $this->readImageDimensions();
        }

        $targetPath = $this->getPath(true);
        rename($this->tmpPath, $targetPath);
        if ($this->size === 0) {
            $this->hash = null;
            unlink($targetPath);
        }
    }

    /**
     * 
     * @return array<string, mixed>
     * @throws NoBinaryException
     * @throws RepoException
     */
    public function getHeaders(): array {
        $query = RC::$pdo->prepare("
            SELECT *
            FROM
                          (SELECT id, value   AS filename FROM metadata WHERE property = ? AND id = ? LIMIT 1) t1
                FULL JOIN (SELECT id, value   AS mime     FROM metadata WHERE property = ? AND id = ? LIMIT 1) t2 USING (id)
                FULL JOIN (SELECT id, value_n AS size     FROM metadata WHERE property = ? AND id = ? LIMIT 1) t3 USING (id)
                FULL JOIN (SELECT id, value   AS hash     FROM metadata WHERE property = ? AND id = ? LIMIT 1) t5 USING (id)
                FULL JOIN (SELECT id, to_char(value_t, 'Dy, DD Mon YYYY HH24:MI:SS GMT') AS moddate  FROM metadata WHERE property = ? AND id = ? LIMIT 1) t4 USING (id)
        ");
        $query->execute([
            RC::$config->schema->fileName, $this->id,
            RC::$config->schema->mime, $this->id,
            RC::$config->schema->binarySize, $this->id,
            RC::$config->schema->hash, $this->id,
            RC::$config->schema->binaryModificationDate, $this->id,
        ]);
        $data  = $query->fetchObject();
        if ($data === false) {
            $data = (object) [
                    'filename' => '',
                    'mime'     => '',
                    'size'     => '',
                    'moddate'  => '',
                    'hash'     => ''
            ];
        }
        $path = $this->getPath();
        if (!empty($data->size) && file_exists($path)) {
            $headers['Content-Length'] = $data->size;
            $headers['Accept-Ranges']  = 'bytes';
        } else {
            throw new NoBinaryException();
        }
        if (!empty($data->filename)) {
            $headers['Content-Disposition'] = 'attachment; filename="' . $data->filename . '"';
        }
        if (!empty($data->mime)) {
            $headers['Content-Type'] = $data->mime;
        }
        if (!empty($data->moddate)) {
            $headers['Last-Modified'] = $data->moddate;
        }
        if (!empty($data->hash)) {
            $headers['ETag'] = '"' . $data->hash . '"';
        }

        return $headers;
    }

    public function getRequestMetadata(): DatasetNode {
        list($contentType, $fileName) = $this->getRequestMetadataRaw();

        $node  = DF::blankNode();
        $graph = new DatasetNode($node);
        $graph->add(DF::quad($node, RC::$schema->mime, DF::literal($contentType)));
        if (!empty($fileName)) {
            $graph->add(DF::quad($node, RC::$schema->fileName, DF::literal($fileName)));
        } else {
            $graph->add(DF::quad($node, RC::$schema->delete, RC::$schema->fileName));
        }
        if ($this->size > 0) {
            $graph->add(DF::quad($node, RC::$schema->binarySize, DF::literal($this->size)));
        } else {
            $graph->add(DF::quad($node, RC::$schema->delete, RC::$schema->binarySize));
        }
        if ($this->size > 0) {
            $graph->add(DF::quad($node, RC::$schema->hash, DF::literal($this->hash)));
        } else {
            $graph->add(DF::quad($node, RC::$schema->delete, RC::$schema->hash));
        }
        if (isset($this->imagePxHeight)) {
            $graph->add(DF::quad($node, RC::$schema->imagePxHeight, DF::literal($this->imagePxHeight)));
        }
        if (isset($this->imagePxWidth)) {
            $graph->add(DF::quad($node, RC::$schema->imagePxWidth, DF::literal($this->imagePxWidth)));
        }
        // Last modification date & user
        $date = (new DateTime())->format('Y-m-d\TH:i:s.u');
        $graph->add(DF::quad($node, RC::$schema->binaryModificationDate, DF::literal($date, null, RDF::XSD_DATE_TIME)));
        $graph->add(DF::quad($node, RC::$schema->binaryModificationUser, DF::literal(RC::$auth->getUserName())));
        return $graph;
    }

    public function backup(string $suffix): void {
        $srcPath = $this->getPath(false);
        $dstPath = $this->getPath(true, $suffix);
        if (file_exists($srcPath)) {
            rename($srcPath, $dstPath);
        } else {
            // mark there was no content
            file_put_contents($dstPath, '');
        }
    }

    public function restore(string $suffix): bool {
        $backupPath = $this->getPath(false, $suffix);
        if (file_exists($backupPath)) {
            $targetPath = $this->getPath(true);
            if (filesize($backupPath) > 0) {
                rename($backupPath, $targetPath);
            } else {
                unlink($backupPath);
                if (file_exists($targetPath)) {
                    unlink($targetPath);
                }
            }
            return true;
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
        $storageDir = self::getStorageDir($this->id, RC::$config->storage->dir, 0, RC::$config->storage->levels);
        if ($create && !file_exists($storageDir)) {
            mkdir($storageDir, (int) base_convert(RC::$config->storage->modeDir, 8, 10), true);
        }
        return "$storageDir/$this->id" . (empty($suffix) ? '' : '.' . $suffix);
    }

    /**
     * 
     * @return array<?string>
     */
    private function getRequestMetadataRaw(): array {
        $contentDisposition = trim((string) filter_input(INPUT_SERVER, 'HTTP_CONTENT_DISPOSITION'));
        $contentType        = (string) filter_input(INPUT_SERVER, 'CONTENT_TYPE');
        RC::$log->debug("\trequest file data - content-type: $contentType, content-disposition: $contentDisposition");

        $fileName = null;
        if (preg_match('/^attachment; filename=/', $contentDisposition)) {
            $fileName = (string) preg_replace('/^attachment; filename="?/', '', $contentDisposition);
            $fileName = (string) preg_replace('/"$/', '', $fileName);
            RC::$log->debug("\t\tfile name: $fileName");
        }

        if (empty($contentType)) {
            if (!empty($fileName)) {
                $contentType = BP::guzzleMimetype($fileName);
                RC::$log->debug("\t\tguzzle mime: $contentType");
                if ($contentType === null) {
                    $contentType = mime_content_type($this->getPath(false));
                    // mime_content_type() doesn't recognize text/plain reliable and may assign it even to binaries
                    $contentType = $contentType === 'text/plain' ? null : $contentType;
                    RC::$log->debug("\t\tmime_content_type mime: $contentType");
                }
            }
            if (empty($contentType)) {
                $contentType = RC::$config->rest->defaultMime;
                RC::$log->debug("\t\tdefault mime: $contentType");
            }
        }
        $contentType = trim((string) preg_replace('/;.*$/', '', $contentType)); // skip additional information, e.g. encoding, version, etc.

        return [$contentType, $fileName];
    }

    private function updateFts(): bool {
        $limit  = $this->toBytes(RC::$config->fullTextSearch->sizeLimits->highlighting);
        $result = false;

        $query = RC::$pdo->prepare("INSERT INTO full_text_search (id, segments, raw) VALUES (?, to_tsvector('simple', ?), ?)");
        $tika  = RC::$config->fullTextSearch->tikaLocation;
        if (substr($tika, 0, 4) === 'http') {
            $client = new Client(['http_errors' => false]);
            $input  = fopen($this->tmpPath, 'r') ?: throw new RepoException("Failed to open binary for indexing");
            $req    = new Request('put', $tika . 'tika', ['Accept' => 'text/plain'], $input);
            $resp   = $client->send($req);
            if ($resp->getStatusCode() === 200) {
                $body    = (string) $resp->getBody();
                $bodyLen = strlen($body);
                if ($bodyLen === 0) {
                    RC::$log->info("\t\tno text extracted");
                }
                $query->execute([$this->id, $body, $bodyLen <= $limit ? $body : null]);
                $result = true;
            }
        } else {
            $output = $ret    = '';
            exec($tika . ' ' . escapeshellarg($this->tmpPath) . ' 2>&1', $output, $ret);
            $output = implode($output);
            if ($ret === 0) {
                $bodyLen = strlen($output);
                if ($bodyLen === 0) {
                    RC::$log->info("\t\tno text extracted");
                }
                $query->execute([$this->id, $output, $bodyLen <= $limit ? $output : null]);
                $result = true;
            } else {
                RC::$log->error("\t\textraction failed with code $ret and message: $output");
            }
        }
        return $result;
    }

    private function updateSpatialSearch(SpatialInterface $spatial): void {
        $query   = sprintf(
            "INSERT INTO spatial_search (id, geom) 
            SELECT ?::bigint, st_transform(st_envelope(geom), 4326)::geography
            FROM (%s) t
            WHERE geom IS NOT NULL",
            $spatial->getSqlQuery()
        );
        $query   = RC::$pdo->prepare($query);
        $content = (string) file_get_contents($this->tmpPath);
        if ($spatial->isInputBinary()) {
            $content = '\x' . bin2hex($content);
        } elseif (substr($content, 0, 3) === hex2bin('EFBBBF')) {
            // skip BOM
            $content = substr($content, 3);
        }
        $query->execute([$this->id, $content]);
    }

    private function readImageDimensions(): void {
        $ret = getimagesize($this->tmpPath);
        if (is_array($ret)) {
            $this->imagePxHeight = $ret[1];
            $this->imagePxWidth  = $ret[0];
        }
    }

    private function toBytes(string $number): int {
        $number = strtolower($number);
        $from   = ['k', 'm', 'g', 't'];
        $to     = ['000', '000000', '000000000', '000000000000'];
        $number = str_replace($from, $to, $number);
        return (int) $number;
    }
}
