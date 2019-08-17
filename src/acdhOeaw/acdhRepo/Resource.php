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

use acdhOeaw\acdhRepo\RestController as RC;
use acdhOeaw\acdhRepo\Transaction;

/**
 * Description of Resource
 *
 * @author zozlak
 */
class Resource {

    const STATE_ACTIVE    = 'active';
    const STATE_TOMBSTONE = 'tombstone';
    const STATE_DELETED   = 'deleted';

    private $id;

    public function __construct(?int $id) {
        $this->id = $id;
    }

    public function optionsMetadata(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, PATCH');
        header('Accept: ' . Metadata::getAcceptedFormats());
    }

    public function headMetadata(bool $get = false): void {
        $this->checkCanRead(true);
        $meta       = new Metadata($this->id);
        $mode       = filter_input(\INPUT_SERVER, RC::getHttpHeaderName('metadataReadMode')) ?? RC::$config->rest->defaultMetadataReadMode;
        $parentProp = filter_input(\INPUT_SERVER, RC::getHttpHeaderName('metadataParentProperty')) ?? RC::$config->schema->parent;
        $meta->loadFromDb(strtolower($mode), $parentProp);
        $format     = $meta->outputHeaders();
        $meta->outputRdf($format);
    }

    public function getMetadata(): void {
        $this->headMetadata(true);
    }

    public function patchMetadata(): void {
        $this->checkCanWrite();
        $meta = new Metadata($this->id);
        $meta->loadFromRequest();
        $mode = filter_input(\INPUT_SERVER, RC::getHttpHeaderName('metadataWriteMode')) ?? RC::$config->rest->defaultMetadataWriteMode;
        $meta->save(strtolower($mode));
        $this->getMetadata();
    }

    public function options(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, PUT, DELETE');
    }

    public function head(): void {
        $this->checkCanRead();
        $binary = new BinaryPayload($this->id);
        $binary->outputHeaders();
    }

    public function get(): void {
        $this->head();
        $binary = new BinaryPayload($this->id);
        $path   = $binary->getPath();
        if (file_exists($path)) {
            readfile($path);
        } else {
            http_response_code(204);
        }
    }

    public function put(): void {
        $this->checkCanWrite();

        $binary = new BinaryPayload($this->id);
        $binary->setKeepAliveHandle([RC::$transaction, 'patch'], RC::$config->transactionController->timeout / 2);
        $binary->upload();

        $meta = new Metadata($this->id);
        $meta->update($binary->getRequestMetadata());
        $meta->save(Metadata::SAVE_MERGE);

        http_response_code(204);
    }

    public function delete(): void {
        $this->checkCanWrite();

        $query = RC::$pdo->prepare("
            UPDATE resources SET state = ? WHERE id = ?
            RETURNING state, transaction_id
        ");
        $query->execute([self::STATE_TOMBSTONE, $this->id]);
        RC::$log->debug($query->fetchObject());

        $binary = new BinaryPayload($this->id);
        $binary->backup(RC::$transaction->getId());

        // delete from relations so it doesn't enforce existence of any other resources
        // keep metadata because they can still store important information, e.g. access rights
        $query = RC::$pdo->prepare("DELETE FROM relations WHERE id = ?");
        $query->execute([$this->id]);

        $meta = new Metadata($this->id);
        $meta->save(Metadata::SAVE_MERGE);

        http_response_code(204);
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

        RC::$log->debug($query->fetchObject());
        http_response_code(204);
    }

    public function optionsCollection(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, POST');
    }

    public function postCollection(): void {
        $this->checkCanCreate();

        $this->createResource();

        $binary = new BinaryPayload($this->id);
        $binary->setKeepAliveHandle([RC::$transaction, 'patch'], RC::$config->transactionController->timeout / 2);
        $binary->upload();

        $meta = new Metadata($this->id);
        $meta->update($binary->getRequestMetadata());
        $meta->update(RC::$auth->getCreateRights());
        $meta->save(Metadata::SAVE_OVERWRITE);

        http_response_code(201);
        header('Location: ' . $this->getUri());
    }

    public function optionsCollectionMetadata(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, POST');
        header('Accept: ' . Metadata::getAcceptedFormats());
    }

    public function postCollectionMetadata(): void {
        $this->checkCanCreate();

        $this->createResource();

        $meta  = new Metadata($this->id);
        $count = $meta->loadFromRequest(RC::getBaseUrl());
        RC::$log->debug("\t$count triples loaded from the user request");
        $meta->update(RC::$auth->getCreateRights());
        $meta->save(Metadata::SAVE_OVERWRITE);

        http_response_code(201);
        header('Location: ' . $this->getUri());
    }

    public function getUri(): string {
        return RC::getBaseUrl() . $this->id;
    }

    public function checkCanRead(bool $metadata = false): void {
        $query = RC::$pdo->prepare("SELECT state FROM resources WHERE id = ?");
        $query->execute([$this->id]);
        $state = $query->fetchColumn();

        if ($state === false || $state === self::STATE_DELETED) {
            throw new RepoException('Not Found', 404);
        }
        if ($state === self::STATE_TOMBSTONE) {
            throw new RepoException('Gone', 410);
        }

        RC::$auth->checkAccessRights($this->id, 'read', $metadata);
    }

    public function checkCanCreate(): void {
        $this->checkTransactionState();
        RC::$auth->checkCreateRights();
    }

    public function checkCanWrite(bool $tombstone = false): void {
        $this->checkTransactionState();

        $txId   = RC::$transaction->getId();
        $query  = RC::$pdo->prepare("
            UPDATE resources 
            SET transaction_id = ?
            WHERE id = ? AND (transaction_id IS NULL OR transaction_id = ?)
            RETURNING state
        ");
        $query->execute([$txId, $this->id, $txId]);
        $result = $query->fetchObject();
        if ($result === false) {
            $query = RC::$pdo->prepare("SELECT state FROM resources WHERE id = ?");
            $query->execute([$this->id]);
            $state = $query->fetchColumn();
            if ($state === false || $state === self::STATE_DELETED) {
                throw new RepoException('Not found', 404);
            } else {
                throw new RepoException('Owned by other transaction', 403);
            }
        }
        if (!$tombstone && $result->state === self::STATE_TOMBSTONE) {
            throw new RepoException('Gone', 410);
        }
        if ($tombstone && $result->state !== self::STATE_TOMBSTONE) {
            throw new RepoException('Not a tombstone', 405);
        }

        RC::$auth->checkAccessRights($this->id, 'write', false);
    }

    private function checkTransactionState(): void {
        $txState = RC::$transaction->getState();
        if (empty($txState)) {
            throw new RepoException('Begin transaction first', 400);
        }
        if ($txState !== Transaction::STATE_ACTIVE) {
            throw new RepoException('Wrong transaction state: ' . $txState, 400);
        }
    }

    private function createResource(): void {
        $query    = RC::$pdo->prepare("INSERT INTO resources (transaction_id) VALUES (?) RETURNING id");
        $query->execute([RC::$transaction->getId()]);
        $this->id = $query->fetchColumn();
        RC::$log->info("\t" . $this->getUri());
    }

}