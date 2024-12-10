<?php

/*
 * The MIT License
 *
 * Copyright 2024 zozlak.
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
use DateTimeImmutable;
use ZipStream\ZipStream;
use acdhOeaw\arche\core\RestController as RC;
use ZipStream\CompressionMethod;

/**
 * Class handling the batch download endpoint
 *
 * @author zozlak
 */
class Download {

    const CONTENT_TYPE                     = 'application/zip';
    const DEFAULT_COMPRESSION_METHOD       = CompressionMethod::STORE;
    const DEFAULT_COMPRESSION_LEVEL        = 1;
    const DEFAULT_STRICT                   = false;
    const DEFAULT_FILE_NAME                = 'data.zip';
    const FORBIDDEN_FILENAME_CHARS_REGEX   = '/[^-_[:alnum:] ]/u';
    const FORBIDDEN_FILENAME_CHARS_REPLACE = '_';

    /**
     * 
     * @var array<string, array<string, string>>
     */
    private array $parents;
    private PDOStatement $parentQuery;

    /**
     * 
     * @var array<mixed>
     */
    private array $parentQueryParam;

    public function get(): void {
        $ids = $_GET['ids'] ?? $_POST['ids'] ?? [];
        if (!is_array($ids)) {
            $ids = [$ids];
        }
        if (count($ids) === 0) {
            throw new RepoException('No resources identifiers provided');
        }

        $allIds = $this->collectChildren($ids);
        unset($ids);

        $skip     = (bool) json_decode($_GET['skipUnauthorized'] ?? false); // so "false" is turned into false
        $validIds = $this->checkAccessRights($allIds, $skip);
        unset($allIds);
        if (count($validIds) === 0) {
            throw new RepoException("Unauthorized to download all requested resources", 403);
        }

        // create a zip
        $cfg      = RC::$config->download;
        $strict   = strtoupper($_GET['strict'] ?? $cfg->strict ?? self::DEFAULT_STRICT);
        $method   = match ($cfg->compressionMethod ?? '') {
            'store' => CompressionMethod::STORE,
            'deflate' => CompressionMethod::DEFLATE,
            default => self::DEFAULT_COMPRESSION_METHOD,
        };
        $level    = $cfg->compressionLevel ?? self::DEFAULT_COMPRESSION_LEVEL;
        $fileName = $cfg->fileName ?? self::DEFAULT_FILE_NAME;

        $metaQuery      = RC::$pdo->prepare("
            SELECT m1.value AS filename, m2.value AS lastmod, m3.value_n AS filesize, r.target_id AS parent
            FROM
                metadata m1
                JOIN metadata m2 USING (id)
                JOIN metadata m3 USING (id)
                LEFT JOIN relations r ON m1.id = r.id AND r.property = ?
            WHERE
                m1.id = ?
                AND m1.property = ?
                AND m2.property = ?
                AND m3.property = ?
        ");
        $metaQueryParam = [
            RC::$schema->parent,
            null,
            RC::$schema->fileName,
            RC::$schema->binaryModificationDate,
            RC::$schema->binarySize,
        ];
        $this->parents  = [];
        unset($this->parentQuery);
        unset($this->parentQueryParam);
        $zip            = new ZipStream(contentType: self::CONTENT_TYPE, defaultCompressionMethod: $method, defaultDeflateLevel: $level, enableZip64: !$strict, defaultEnableZeroHeader: !$strict, outputName: $fileName);
        foreach ($validIds as $id) {
            $binary = new BinaryPayload($id);
            $path   = $binary->getPath();
            if (!file_exists($path)) {
                continue; // metadata-only resource
            }
            $metaQueryParam[1] = $id;
            $metaQuery->execute($metaQueryParam);
            $meta              = $metaQuery->fetchObject();
            $this->fetchParentsMeta($id, $meta);
            $filename          = $meta->filename;
            $pid               = (string) $meta->parent;
            while (!empty($pid)) {
                $filename = $this->parents[$pid]['filename'] . '/' . $filename;
                $pid      = $this->parents[$pid]['parent'];
            }
            $zip->addFileFromPath($filename, $path, lastModificationDateTime: new DateTimeImmutable($meta->lastmod), exactSize: $meta->filesize);
        }
        $zip->finish();
    }

    public function options(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, POST');
    }

    private function fetchParentsMeta(int $id, object $meta): void {
        $this->parentQuery      ??= RC::$pdo->prepare("
            SELECT r.id, n, COALESCE(m1.value, m2.value) AS filename
            FROM
                get_relatives(?, ?, 0, -999999, false, false) r
                LEFT JOIN metadata m1 ON r.id = m1.id AND m1.property = ?
                LEFT JOIN metadata m2 ON r.id = m2.id AND m2.property = ?
            ORDER BY n DESC
        ");
        $this->parentQueryParam ??= [
            null,
            RC::$schema->parent,
            RC::$schema->fileName,
            RC::$schema->label,
        ];
        if ($meta->parent !== null && !isset($this->parents[$meta->parent])) {
            $this->parentQueryParam[0] = $id;
            $this->parentQuery->execute($this->parentQueryParam);
            $parentsMeta               = $this->parentQuery->fetchAll(PDO::FETCH_OBJ);
            for ($i = 0; $i < count($parentsMeta); $i++) {
                $pid = (string) $parentsMeta[$i]->id;
                if (isset($tthis->parents[$pid])) {
                    break;
                }
                $this->parents[$pid] = [
                    'filename' => preg_replace(self::FORBIDDEN_FILENAME_CHARS_REGEX, self::FORBIDDEN_FILENAME_CHARS_REPLACE, $parentsMeta[$i]->filename),
                    'parent'   => (string) ($parentsMeta[$i + 1] ?? null)?->id,
                ];
            }
        }
    }

    /**
     * 
     * @param array<string> $ids
     * @return array<string>
     */
    private function collectChildren(array $ids): array {
        $baseUrl = RC::$config->rest->urlBase . RC::$config->rest->pathBase;
        $query   = RC::$pdo->prepare("
            SELECT gr.id 
            FROM 
                identifiers i,
                LATERAL get_relatives(i.id, ?, 999999, 0, false, false) gr
            WHERE
                i.ids = ?
                AND EXISTS (SELECT 1 FROM metadata WHERE id = gr.id AND property = ?)
        ");
        $param   = [RC::$schema->parent, null, RC::$schema->binarySize];
        $allIds  = [];
        foreach ($ids as $id) {
            $param[1] = is_numeric($id) ? $baseUrl . $id : $id;
            $query->execute($param);
            while ($i        = $query->fetchColumn()) {
                $allIds[(string) $i] = '';
            }
        }
        return $allIds;
    }

    /**
     * 
     * @param array<string> $ids
     * @return array<int>
     */
    private function checkAccessRights(array $ids, bool $skipUnauthorized): array {
        $validIds = [];
        foreach (array_keys($ids) as $id) {
            try {
                RC::$log->debug("Testing $id");
                $id         = (int) $id;
                RC::$auth->checkAccessRights($id, 'read', false);
                RC::$log->debug("    passed");
                $validIds[] = $id;
            } catch (RepoException $e) {
                if (!$skipUnauthorized || !in_array($e->getCode(), [401, 403])) {
                    throw $e;
                }
            }
        }
        return $validIds;
    }
}
