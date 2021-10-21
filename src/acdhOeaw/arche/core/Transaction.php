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

use PDO;
use PDOException;
use RuntimeException;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Description of Transaction
 *
 * @author zozlak
 */
class Transaction {

    const STATE_ACTIVE             = 'active';
    const STATE_COMMIT             = 'commit';
    const STATE_ROLLBACK           = 'rollback';
    const PG_FOREIGN_KEY_VIOLATION = 23503;

    private ?int $id          = null;
    private string $startedAt;
    private string $lastRequest = '';
    private ?string $state       = null;
    private string $snapshot;

    /**
     * Database connection.
     * A separate is required so it can commit changes independently from the main connection.
     */
    private PDO $pdo;

    public function __construct() {
        $this->pdo = new PDO(RC::$config->dbConn->admin);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->query("SET application_name TO rest_tx");

        header('Cache-Control: no-cache');
        $this->id = RC::getRequestParameter('transactionId');
        if ($this->id !== null) {
            $this->fetchData();
        }
    }

    public function prolongAndRelease(): void {
        if (!empty($this->id)) {
            $query = $this->pdo->prepare("UPDATE transactions SET last_request = clock_timestamp() WHERE transaction_id = ? RETURNING last_request");
            $query->execute([$this->id]);
            RC::$log->debug('Updating ' . $this->id . ' transaction timestamp with ' . $query->fetchColumn());
        }

        if ($this->pdo->inTransaction()) {
            $this->pdo->commit();
        }
    }

    public function getId(): ?int {
        return $this->id;
    }

    public function getState(): ?string {
        return $this->state;
    }

    public function options(int $code = 204): void {
        http_response_code($code);
        header('Allow: OPTIONS, POST, HEAD, GET, PUT, DELETE');
    }

    public function head(): void {
        if (!isset($this->id)) {
            throw new RepoException('Unknown transaction', 400);
        }
        header(substr(RC::getHttpHeaderName('transactionId'), 5) . ': ' . $this->id);
        header('Content-Type: application/json');
    }

    public function get(): void {
        $this->head();
        echo json_encode([
            'transactionId' => $this->id,
            'startedAt'     => $this->startedAt,
            'lastRequest'   => $this->lastRequest,
            'state'         => $this->state,
        ]) . "\n";
    }

    public function delete(): void {
        if (!isset($this->id)) {
            throw new RepoException('Unknown transaction', 400);
        }

        $this->prolongAndRelease();
        RC::$handlersCtl->handleTransaction('rollback', (int) $this->id, $this->getResourceList());

        $query = $this->pdo->prepare("
            UPDATE transactions SET state = 'rollback' WHERE transaction_id = ?
        ");
        $query->execute([$this->id]);

        $this->wait();
        http_response_code(204);
    }

    public function put(): void {
        if (!isset($this->id)) {
            throw new RepoException('Unknown transaction', 400);
        }

        RC::$handlersCtl->handleTransaction('commit', (int) $this->id, $this->getResourceList());

        RC::$log->debug('Cleaning up transaction ' . $this->id);
        try {
            $query = $this->pdo->prepare("
                DELETE FROM resources
                WHERE transaction_id = ? AND state = ?
            ");
            $query->execute([$this->id, Resource::STATE_DELETED]);

            $query = $this->pdo->prepare("
                UPDATE transactions SET state = ? WHERE transaction_id = ?
            ");
            $query->execute([self::STATE_COMMIT, $this->id]);
            http_response_code(204);
        } catch (PDOException $e) {
            RC::$log->error($e->getMessage());
            if ((int) $e->getCode() === self::PG_FOREIGN_KEY_VIOLATION) {
                throw new RepoException('Foreign constraint conflict', 409);
            } else {
                throw $e;
            }
        }

        $this->wait();
    }

    public function post(): void {
        try {
            $this->id = TransactionController::registerTransaction(RC::$config);
        } catch (RepoLibException $e) {
            throw new RuntimeException('Transaction creation failed', 500, $e);
        }

        RC::$handlersCtl->handleTransaction('begin', $this->id, []);

        http_response_code(201);
        $this->fetchData();
        $this->get();
    }

    public function getPreTransactionDbHandle(): PDO {
        $pdo = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, 1);
        $pdo->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '" . $this->snapshot . "'");
        $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);
        return $pdo;
    }

    private function lock(): void {
        $this->pdo->beginTransaction();
        $query = $this->pdo->prepare("
                SELECT * FROM transactions WHERE transaction_id = ? FOR UPDATE
            ");
        $query->execute([$this->id]);
    }

    private function fetchData(): void {
        $query = $this->pdo->prepare("
            UPDATE transactions SET last_request = clock_timestamp() WHERE transaction_id = ?
            RETURNING started, last_request AS last, state, snapshot
        ");
        $query->execute([$this->id]);
        $data  = $query->fetchObject();
        if ($data !== false) {
            $this->startedAt   = $data->started;
            $this->lastRequest = $data->last;
            $this->state       = $data->state;
            $this->snapshot    = $data->snapshot;
            RC::$log->debug('Updating ' . $this->id . ' transaction timestamp with ' . $this->lastRequest);
        }
    }

    /**
     * Actively waits until the transaction controller daemon rollbacks/commits the transaction
     */
    private function wait(): void {
        if ($this->pdo->inTransaction()) {
            $this->pdo->commit();
        }
        $query = $this->pdo->prepare("SELECT * FROM transactions WHERE transaction_id = ?");
        do {
            usleep(1000 * RC::$config->transactionController->checkInterval / 4);
            RC::$log->debug('Waiting for the transaction ' . $this->id . ' to end');
            $query->execute([$this->id]);
            $exists = $query->fetchObject() !== false;
        } while ($exists);
        RC::$log->info('Transaction ' . $this->id . ' ended');
    }

    /**
     * 
     * @return array<int>
     */
    private function getResourceList(): array {
        $query = $this->pdo->prepare("SELECT id FROM resources WHERE transaction_id = ?");
        $query->execute([$this->id]);
        return $query->fetchAll(PDO::FETCH_COLUMN) ?: [];
    }
}
