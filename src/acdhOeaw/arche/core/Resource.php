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

use Throwable;
use EasyRdf\Graph;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\Transaction;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Description of Resource
 *
 * @author zozlak
 */
class Resource {

    const STATE_ACTIVE    = 'active';
    const STATE_TOMBSTONE = 'tombstone';
    const STATE_DELETED   = 'deleted';

    private ?int $id;

    public function __construct(?int $id) {
        $this->id = $id;
    }

    public function optionsMetadata(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, PATCH');
        header('Accept: ' . Metadata::getAcceptedFormats());
    }

    public function headMetadata(bool $get = false): void {
        $this->checkCanRead();
        RC::$auth->checkAccessRights((int) $this->id, 'read', true);
        $format = Metadata::outputHeaders();
        if ($get) {
            $meta       = new MetadataReadOnly((int) $this->id);
            $mode       = RC::getRequestParameter('metadataReadMode') ?? RC::$config->rest->defaultMetadataReadMode;
            $parentProp = RC::getRequestParameter('metadataParentProperty') ?? RC::$config->schema->parent;
            $meta->loadFromDb(strtolower($mode), $parentProp);
            $meta->outputRdf($format);
        }
    }

    public function getMetadata(): void {
        $this->headMetadata(true);
    }

    public function patchMetadata(): void {
        $this->checkCanWrite();
        $meta = new Metadata((int) $this->id);
        $meta->loadFromRequest();
        $mode = RC::getRequestParameter('metadataWriteMode') ?? RC::$config->rest->defaultMetadataWriteMode;
        $meta->merge(strtolower($mode));
        $meta->loadFromResource(RC::$handlersCtl->handleResource('updateMetadata', (int) $this->id, $meta->getResource(), null));
        $meta->save();
        $this->getMetadata();
    }

    /**
     * Merges the $srcId resource into the current resource.
     * 
     * Preserves identifiers and unique properties from both. Non-unique properties
     * are kept from the current resource only.
     * 
     * @param int $srcId
     * @return void
     */
    public function merge(int $srcId): void {
        $this->checkCanWrite();
        $srcRes = new Resource($srcId);
        $srcRes->checkCanWrite();

        $srcMeta    = new Metadata($srcId);
        $srcMeta->loadFromDb(RRI::META_RESOURCE);
        $srcMeta    = $srcMeta->getResource();
        $targetMeta = new Metadata($this->id);
        $targetMeta->loadFromDb(RRI::META_RESOURCE);

        $meta = $targetMeta->getResource();
        foreach (array_diff($meta->propertyUris(), [RC::$config->schema->id]) as $p) {
            $srcMeta->delete($p);
        }
        $meta->merge($srcMeta, [RC::$config->schema->id]);
        RC::$log->debug("\n" . $meta->getGraph()->serialise('turtle'));
        $meta = RC::$handlersCtl->handleResource('updateMetadata', (int) $this->id, $meta, null);
        $targetMeta->loadFromResource($meta);

        $query = RC::$pdo->prepare("DELETE FROM resources WHERE id = ?");
        $query->execute([$srcId]);
        $targetMeta->save();
        $this->headMetadata(true);
    }

    public function options(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, PUT, DELETE');
    }

    public function head(): void {
        $this->checkCanRead();
        try {
            $binary  = new BinaryPayload((int) $this->id);
            $headers = $binary->getHeaders();
            RC::$auth->checkAccessRights((int) $this->id, 'read', false);
            foreach ($headers as $h => $v) {
                header("$h: $v");
            }
        } catch (NoBinaryException $e) {
            http_response_code(302);
            header('Location: ' . $this->getUri() . '/metadata');
        }
    }

    public function get(): void {
        $this->head();
        $binary = new BinaryPayload((int) $this->id);
        $path   = $binary->getPath();
        if (file_exists($path)) {
            readfile($path);
        }
    }

    public function put(): void {
        $this->checkCanWrite();

        $binary = new BinaryPayload((int) $this->id);
        $binary->upload();

        $meta = new Metadata($this->id);
        $meta->update($binary->getRequestMetadata());
        $meta->merge(Metadata::SAVE_MERGE);
        $meta->loadFromResource(RC::$handlersCtl->handleResource('updateBinary', (int) $this->id, $meta->getResource(), $binary->getPath()));
        $meta->save();

        http_response_code(204);
    }

    public function delete(): void {
        $this->checkCanWrite();

        $txId       = RC::$transaction->getId();
        $parentProp = RC::getRequestParameter('metadataParentProperty');
        $delRefs    = RC::getRequestParameter('withReferences');

        if (empty($parentProp)) {
            $resQuery = "SELECT ?::bigint AS id";
            $resParam = [$this->id];
        } else {
            $resQuery = "SELECT * FROM get_relatives(?, ?, 999999, 0)";
            $resParam = [$this->id, $parentProp];
        }
        $query = RC::$pdo->prepare("CREATE TEMPORARY TABLE delres AS $resQuery");
        $query->execute($resParam);

        $this->deleteLockAll($txId);
        $this->deleteCheckReferences($txId, (bool) ((int) $delRefs));
        RC::$auth->batchCheckAccessRights('delres', 'write', false);

        $graph  = new Graph();
        $idProp = RC::$config->schema->id;
        $base   = RC::getBaseUrl();
        $query  = RC::$pdo->query("SELECT id, i.ids FROM identifiers i JOIN delres USING (id)");
        while ($res    = $query->fetchObject()) {
            $graph->resource($base . $res->id)->addResource($idProp, $res->ids);
        }
        $format = Metadata::negotiateFormat();
        Metadata::outputHeaders($format);
        echo $graph->serialise($format);

        $this->deleteReferences();
        $this->deleteResources($txId);
    }

    public function optionsTombstone(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, DELETE');
    }

    public function deleteTombstone(): void {
        $this->checkCanWrite(true);

        $query = RC::$pdo->prepare("
            UPDATE resources SET state = ? WHERE id = ? 
            RETURNING state, transaction_id
        ");
        $query->execute([self::STATE_DELETED, $this->id]);

        $meta = new Metadata($this->id);
        $meta->loadFromDb(RRI::META_RESOURCE);
        RC::$handlersCtl->handleResource('deleteTombstone', (int) $this->id, $meta->getResource(), null);

        RC::$log->debug(json_encode($query->fetchObject()));
        http_response_code(204);
    }

    public function optionsCollection(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, POST');
    }

    public function postCollection(): void {
        $this->checkCanCreate();

        $this->id = RC::$transaction->createResource(RC::$logId);
        try {
            $binary = new BinaryPayload((int) $this->id);
            $binary->upload();

            $meta = new Metadata($this->id);
            $meta->update($binary->getRequestMetadata());
            $meta->update(RC::$auth->getCreateRights());
            $meta->merge(Metadata::SAVE_OVERWRITE);
            $meta->loadFromResource(RC::$handlersCtl->handleResource('create', (int) $this->id, $meta->getResource(), $binary->getPath()));
            $meta->save(true);

            header('Location: ' . $this->getUri());
            http_response_code(201);
            $this->getMetadata();
        } catch (Throwable $e) {
            RC::$transaction->deleteResource($this->id);
            throw $e;
        }
    }

    public function optionsCollectionMetadata(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, POST');
        header('Accept: ' . Metadata::getAcceptedFormats());
    }

    public function postCollectionMetadata(): void {
        $this->checkCanCreate();

        $idProp   = RC::$config->schema->id;
        $meta     = new Metadata();
        $count    = $meta->loadFromRequest(RC::getBaseUrl());
        RC::$log->debug("\t$count triples loaded from the user request");
        $metaRes  = $meta->getResource();
        $ids      = Metadata::propertyAsString($metaRes, $idProp);
        $this->id = RC::$transaction->createResource(RC::$logId, $ids);
        try {
            $meta->setId($this->id);
            $meta->update(RC::$auth->getCreateRights());
            $meta->merge(Metadata::SAVE_OVERWRITE);
            $meta->loadFromResource(RC::$handlersCtl->handleResource('create', (int) $this->id, $meta->getResource(), null));
            $meta->save(true);

            header('Location: ' . $this->getUri());
            http_response_code(201);
            $this->getMetadata();
        } catch (Throwable $e) {
            RC::$transaction->deleteResource($this->id);
            throw $e;
        }
    }

    public function getUri(): string {
        return RC::getBaseUrl() . $this->id;
    }

    public function checkCanRead(): void {
        $query = RC::$pdo->prepare("SELECT state FROM resources WHERE id = ?");
        $query->execute([$this->id]);
        $state = $query->fetchColumn();

        if ($state === false || $state === self::STATE_DELETED) {
            throw new RepoException('Not Found', 404);
        }
        if ($state === self::STATE_TOMBSTONE) {
            throw new RepoException('Gone', 410);
        }
    }

    public function checkCanCreate(): void {
        $this->checkTransactionState();
        RC::$auth->checkCreateRights();
    }

    public function checkCanWrite(bool $tombstone = false): void {
        $this->checkTransactionState();

        // Lock by marking lock and transaction_id columns in the resources table
        // It makes it possible for other requests to determine the resource is
        // locked in a non-blocking way.
        $state = RC::$transaction->lockResource($this->id, RC::$logId);
        if ($state === self::STATE_DELETED) {
            throw new RepoException('Not Found', 404);
        }
        if (!$tombstone && $state === self::STATE_TOMBSTONE) {
            throw new RepoException('Gone', 410);
        }
        if ($tombstone && $state !== self::STATE_TOMBSTONE) {
            throw new RepoException('Not a tombstone', 405);
        }

        RC::$auth->checkAccessRights((int) $this->id, 'write', false);

        // Lock by obtaining a row lock on the database
        $query = RC::$pdo->prepare("SELECT id FROM resources WHERE id = ? FOR UPDATE");
        $query->execute([$this->id]);
    }

    private function checkTransactionState(): void {
        $txState = RC::$transaction->getState();
        switch ($txState) {
            case Transaction::STATE_ACTIVE:
                break;
            case Transaction::STATE_NOTX:
                throw new RepoException('Begin transaction first', 400);
            default:
                throw new RepoException("Wrong transaction state: $txState", 400);
        }
    }

    private function deleteLockAll(int $txId): void {
        // try to lock all resources to be deleted
        $query     = "
            WITH t AS (
                UPDATE resources r
                SET transaction_id = ?
                WHERE
                    EXISTS (SELECT 1 FROM delres WHERE id = r.id)
                    AND (transaction_id = ? OR transaction_id IS NULL)
                RETURNING *
            )
            SELECT 
                coalesce(string_agg(CASE transaction_id IS NULL WHEN true THEN id::text ELSE null::text END, ', ' ORDER BY id), '') AS notlocked
            FROM
                delres
                LEFT JOIN t USING (id)
            WHERE transaction_id IS NULL
        ";
        $query     = RC::$pdo->prepare($query);
        $query->execute([$txId, $txId]);
        $notlocked = $query->fetchColumn();
        if (!empty($notlocked)) {
            throw new RepoException("Deleted resource(s) owned by other transaction: $notlocked", 409);
        }
    }

    private function deleteCheckReferences(int $txId, bool $delRefs): void {
        // check references
        RC::$pdo->query("
            CREATE TEMPORARY TABLE delrel AS (
                SELECT *
                FROM relations r
                WHERE
                    EXISTS (SELECT 1 FROM delres WHERE id = r.target_id)
                    AND NOT EXISTS (SELECT 1 FROM delres WHERE id = r.id)
            )
        ");
        $query = "
            WITH t AS (
                UPDATE resources r
                SET transaction_id = ?
                WHERE
                    EXISTS (SELECT 1 FROM delrel WHERE id = r.id)
                    AND (transaction_id = ? OR transaction_id IS NULL)
                RETURNING *
            )
            SELECT
                count(*) AS count,
                coalesce(string_agg(CASE transaction_id IS NULL WHEN true THEN id::text ELSE null::text END, ', ' ORDER BY id), '') AS notlocked
            FROM
                delrel
                LEFT JOIN t USING (id)
        ";
        $query = RC::$pdo->prepare($query);
        $query->execute([$txId, $txId]);
        $res   = $query->fetchObject();
        if ($res->count > 0 && $delRefs === false) {
            throw new RepoException('Referenced by other resource(s)', 409);
        }
        if (!empty($res->notlocked)) {
            throw new RepoException("Referencing resource(s) owned by other transaction: $res->notlocked", 409);
        }
    }

    private function deleteReferences(): void {
        $query  = RC::$pdo->query("
            WITH d AS (
                DELETE FROM relations 
                WHERE (id, target_id, property) IN (SELECT id, target_id, property FROM delrel)
                RETURNING *
            )
            SELECT DISTINCT id FROM d
        ");
        $errors = '';
        if (RC::$handlersCtl->hasHandlers('updateMetadata')) {
            while ($id = $query->fetchColumn()) {
                $meta = new Metadata($id);
                $meta->loadFromDb(RRI::META_RESOURCE);
                try {
                    $meta->loadFromResource(RC::$handlersCtl->handleResource('updateMetadata', $id, $meta->getResource(), null));
                    $meta->save();
                } catch (RepoLibException $e) {
                    $errors .= "Error while removing reference from resource $id: " . $e->getMessage() . "\n";
                }
            }
        }
        if (!empty($errors)) {
            throw new RepoException($errors, 400);
        }
    }

    private function deleteResources(int $txId): void {
        $query = RC::$pdo->prepare("
            WITH d AS (
                UPDATE resources 
                SET state = ? 
                WHERE id IN (SELECT id FROM delres)
                RETURNING id
            )
            SELECT string_agg(id::text, ', ') AS removed FROM d
        ");
        $query->execute([self::STATE_TOMBSTONE]);
        RC::$log->debug($query->fetchColumn());

        // delete from relations and identifiers so it doesn't enforce/block existence of any other resources
        // keep metadata because they can still store important information, e.g. access rights
        RC::$pdo->query("DELETE FROM relations WHERE id IN (SELECT id FROM delres)");
        RC::$pdo->query("DELETE FROM identifiers WHERE id IN (SELECT id FROM delres)");

        $query = RC::$pdo->query("SELECT id FROM delres ORDER BY id");
        while ($id    = $query->fetchColumn()) {
            $binary = new BinaryPayload($id);
            $binary->backup((string) $txId);

            if (RC::$handlersCtl->hasHandlers('delete')) {
                $meta = new Metadata($id);
                $meta->loadFromDb(RRI::META_RESOURCE);
                RC::$handlersCtl->handleResource('delete', $id, $meta->getResource(), $binary->getPath());
                $meta->merge(Metadata::SAVE_MERGE);
                $meta->save();
            }
        }
    }
}
