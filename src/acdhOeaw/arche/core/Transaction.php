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
use PDOStatement;
use RuntimeException;
use Throwable;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Description of Transaction
 *
 * @author zozlak
 */
class Transaction {

    const STATE_NOTX               = 'notx';
    const STATE_ACTIVE             = 'active';
    const STATE_COMMIT             = 'commit';
    const STATE_ROLLBACK           = 'rollback';
    const STATE_LOCKED             = 'locked';
    const PG_FOREIGN_KEY_VIOLATION = '23503';
    const PG_LOCK_FAILURE          = '55P03';
    const PG_DUPLICATE_KEY         = '23505';
    const PG_WRONG_DATE_VALUE      = '22007';
    const LOCK_TIMEOUT_DEFAULT     = 10000;
    const STMT_TIMEOUT_DEFAULT     = 60000;

    private ?int $id              = null;
    private string $startedAt       = '';
    private string $lastRequest     = '';
    private string $state           = self::STATE_NOTX;
    private string $snapshot;
    private bool $lockedResources = false;

    /**
     * Database connection.
     * A separate is required so it can commit changes independently from the main connection.
     */
    private PDO $pdo;

    public function __construct() {
        $this->pdo   = new PDO(RC::$config->dbConn->admin);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->query("SET application_name TO rest_tx_" . RC::$logId);
        $lockTimeout = (int) (RC::$config->transactionController->lockTimeout ?? self::LOCK_TIMEOUT_DEFAULT);
        $this->pdo->query("SET lock_timeout TO $lockTimeout");

        $id       = (int) RC::getRequestParameter('transactionId');
        $this->id = $id > 0 ? $id : null;
        if ($this->id !== null) {
            header('Cache-Control: no-cache');
            $this->fetchData();
        }
    }

    public function prolong(): void {
        if ($this->id === null) {
            return;
        }
        $query = $this->pdo->prepare("
            UPDATE transactions 
            SET last_request = clock_timestamp() 
            WHERE transaction_id = ?
            RETURNING last_request
        ");
        $query->execute([$this->id]);
        RC::$log->debug("Updating $this->id transaction timestamp with " . $query->fetchColumn());
    }

    public function deleteResource(int $resId): void {
        if ($this->pdo->inTransaction()) {
            throw new RuntimeException("Can't delete a resource while inside a database transaction");
        }
        $query = $this->pdo->prepare("DELETE FROM resources WHERE id = ?");
        $query->execute([$resId]);
    }

    public function createResource(int $lockId, array $ids = []): int {
        if ($this->pdo->inTransaction()) {
            throw new RuntimeException("Can't lock a resource while inside a database transaction");
        }
        $this->pdo->beginTransaction();

        $this->lock(false);

        $query = $this->pdo->prepare("
            INSERT INTO resources (transaction_id, lock)
            VALUES (?, ?)
            RETURNING id
        ");
        $query->execute([$this->id, $lockId]);
        $resId = $query->fetchColumn();

        $ids[] = Metadata::idAsUri($resId);
        try {
            $query = $this->pdo->prepare("INSERT INTO identifiers (ids, id) VALUES (?, ?)");
            foreach ($ids as $i) {
                $query->execute([$i, $resId]);
            }
        } catch (PDOException $e) {
            $this->pdo->rollBack();
            if ($e->getCode() === self::PG_DUPLICATE_KEY) {
                throw new DuplicatedKeyException($e->getMessage());
            }
            throw $e;
        }

        $this->pdo->commit();

        $this->lockedResources = true;
        RC::$log->debug("Resource $resId created with transaction $this->id and lock $lockId");
        return $resId;
    }

    /**
     * Locks a given resource with a given lock id and the current transaction id.
     * 
     * - First a database lock on the transaction is obtained with `$this->lock(false)`.
     * - Then a database lock on the resource is obtained and the resource is
     *   marked as arche-locked by setting up `transaction_id` and `lock`
     *   column values
     * - Finally the database transaction is commited.
     * 
     * @param int $resId
     * @param int $lockId
     * @return string|null
     * @throws RuntimeException
     * @throws BadRequestException
     * @throws ConflictException
     */
    public function lockResource(int $resId, int $lockId): ?string {
        if ($this->pdo->inTransaction()) {
            throw new RuntimeException("Can't lock a resource while inside a database transaction");
        }
        $this->pdo->beginTransaction();

        $this->lock(false);
        $query = $this->pdo->prepare("
            SELECT state, lock, transaction_id AS txid
            FROM resources r
            WHERE id = ? 
            FOR UPDATE NOWAIT
        ");
        $this->executeQuery($query, [$resId], "Resource $resId locked");
        $data  = $query->fetchObject();
        if ($data === false) {
            $this->pdo->rollBack();
            throw new BadRequestException('Not found', 404);
        } elseif ($data->txid !== null && $data->txid !== $this->id) {
            $this->pdo->rollBack();
            throw new BadRequestException('Owned by other transaction', 403);
        } elseif ($data->lock !== null && $data->lock !== $lockId) {
            $this->pdo->rollBack();
            throw new ConflictException('Owned by other request');
        }

        $query = $this->pdo->prepare("
            UPDATE resources r
            SET lock = ?, transaction_id = ?
            WHERE id = ?
        ");
        $query->execute([$lockId, $this->id, $resId]);

        $this->pdo->commit();

        $this->lockedResources = true;
        RC::$log->debug("Resource $resId locked with transaction $this->id and lock $lockId");

        return $data->state;
    }

    public function unlockResources(int $lockId): void {
        if (!$this->lockedResources) {
            return;
        }
        $inTx = (int) $this->pdo->inTransaction();
        if ($inTx) {
            RC::$log->warning("Calling Transaction::unlockResources() while it's PDO handler is inside a transaction - it will likely fail");
        }
        $query = $this->pdo->prepare("
            UPDATE resources 
            SET lock = null
            WHERE lock = ?
        ");
        $this->executeQuery($query, [$lockId], "Failed to release resource locks");
        RC::$log->debug("Resource locks released");
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
        if ($this->id === null) {
            throw new RepoException('Unknown transaction', 400);
        }
        header(RC::$config->rest->headers->transactionId . ': ' . $this->id);
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
        if ($this->id === null) {
            throw new RepoException('Unknown transaction', 400);
        }

        try {
            $this->pdo->beginTransaction();
            $this->lock(true);

            RC::$handlersCtl->handleTransaction('rollback', (int) $this->id, $this->getResourceList());

            $this->setState(self::STATE_ROLLBACK);
        } catch (Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
        $this->releaseAndWait();
        http_response_code(204);
    }

    public function put(): void {
        if ($this->id === null) {
            throw new RepoException('Unknown transaction', 400);
        }

        try {
            $this->pdo->beginTransaction();
            $this->lock(true);

            RC::$handlersCtl->handleTransaction('commit', (int) $this->id, $this->getResourceList());

            RC::$log->debug('Cleaning up transaction ' . $this->id);
            $query = $this->pdo->prepare("
                DELETE FROM resources
                WHERE transaction_id = ? AND state = ?
            ");
            $param = [$this->id, Resource::STATE_DELETED];
            $this->executeQuery($query, $param, "Foreign constraint conflict", true);

            $this->setState(self::STATE_COMMIT);
        } catch (Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
        $this->releaseAndWait();
        http_response_code(204);
    }

    public function post(): void {
        header('Cache-Control: no-cache');
        try {
            $this->id = TransactionController::registerTransaction(RC::$config);
        } catch (RepoLibException $e) {
            throw new RuntimeException('Transaction creation failed', 500, $e);
        }

        try {
            $this->pdo->beginTransaction();
            $this->lock(true);

            RC::$handlersCtl->handleTransaction('begin', $this->id, []);

            $this->pdo->commit();
        } catch (Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
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

    /**
     * Tries to lock the current transaction.
     * Assures no other transaction PUT(commit)/DELETE(rollback) is executed 
     * in parallel. Will succeed only if there's no this transaction related
     * resource operation being executed.
     * 
     * Lock persists until $this->pdo->commit()/rollBack();
     * 
     * @param bool $checkResourceLocks
     * @return void
     * @throws ConflictException
     * @throws PDOException
     */
    private function lock(bool $checkResourceLocks = true): void {
        if (!$this->pdo->inTransaction()) {
            throw new RuntimeException("Must be in a database transaction");
        }
        $resLocksClasue = '';
        if ($checkResourceLocks) {
            $resLocksClasue = "
                AND NOT EXISTS (
                    SELECT 1
                    FROM resources
                    WHERE
                        transaction_id = t.transaction_id
                        AND lock IS NOT NULL
                )
            ";
        }
        // Can't use NOWAIT becuase even GET on transaction modifies it (updates 
        // the last_request db field) and NOWAIT would cause one of two parallel
        // transaction GET to fail. Instead rely on the lock_timeout being set
        // on the connecitn in the Transaction class constructor.
        $query = $this->pdo->prepare("
            SELECT state
            FROM transactions t
            WHERE
                transaction_id = ?
                $resLocksClasue
            FOR UPDATE
        ");
        $this->executeQuery($query, [$this->id], "Transaction $this->id locked");
        $state = $query->fetchColumn();
        if ($state === false) {
            throw new ConflictException("Transaction $this->id can't be locked - there's at least one request belonging to the transaction which is still being processed");
        }
        if ($state !== self::STATE_ACTIVE) {
            $this->state = $state;
            throw new ConflictException("Transaction $this->id is in $state state and can't be locked");
        }
        RC::$log->debug("Transaction $this->id locked");
    }

    private function fetchData(): void {
//        $query = $this->pdo->prepare("
//            UPDATE transactions SET last_request = clock_timestamp() WHERE transaction_id = ?
//            RETURNING started, last_request AS last, state, snapshot
//        ");
        $query = $this->pdo->prepare("
            SELECT started, clock_timestamp() AS last, state, snapshot
            FROM transactions
            WHERE transaction_id = ?
        ");
        $query->execute([$this->id]);
        $data  = $query->fetchObject();
        if ($data === false) {
            throw new BadRequestException("Transaction $this->id doesn't exist");
        }
        $this->startedAt   = $data->started;
        $this->lastRequest = $data->last;
        $this->state       = $data->state;
        $this->snapshot    = $data->snapshot;
        RC::$log->debug('Updating ' . $this->id . ' transaction timestamp with ' . $this->lastRequest);
    }

    /**
     * Actively waits until the transaction controller daemon rollbacks/commits the transaction
     */
    private function releaseAndWait(): void {
        if (!$this->pdo->inTransaction()) {
            throw new RuntimeException("Waiting for unlocked transaction doesn't guarantee atomicity");
        }
        $this->pdo->commit();
        RC::$log->debug("Transaction $this->id released");
        $query = $this->pdo->prepare("SELECT * FROM transactions WHERE transaction_id = ?");
        do {
            usleep(1000 * RC::$config->transactionController->checkInterval / 4);
            RC::$log->debug("Waiting for the transaction $this->id to end");
            $query->execute([$this->id]);
            $exists = $query->fetchObject() !== false;
        } while ($exists);
        RC::$log->info('Transaction ' . $this->id . ' ended');
    }

    private function setState(string $state): void {
        $query       = $this->pdo->prepare("
            UPDATE transactions 
            SET state = ?, last_request = clock_timestamp() 
            WHERE transaction_id = ?
        ");
        $query->execute([$state, $this->id]);
        $this->state = $state;
        RC::$log->debug("Transaction $this->id state changed to $state");
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

    /**
     * Executes a given PDO statement trapping the lock timeout and foreign key
     * database exceptions and turning them it into the `ConflictException`.
     * 
     * @param PDOStatement $query
     * @param array $param
     * @param string $errorMsg
     * @param bool $logException
     * @return void
     * @throws ConflictException
     * @throws PDOException
     */
    private function executeQuery(PDOStatement $query, array $param,
                                  string $errorMsg = "Database lock timeout",
                                  bool $logException = false): void {
        try {
            $query->execute($param);
        } catch (PDOException $e) {
            if ($logException) {
                RC::$log->error($e);
            }
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            switch ((string) $e->getCode()) {
                case self::PG_LOCK_FAILURE:
                    throw new ConflictException($errorMsg);
                case self::PG_FOREIGN_KEY_VIOLATION:
                    throw new ConflictException($errorMsg);
                default:
                    throw $e;
            }
        }
    }
}
