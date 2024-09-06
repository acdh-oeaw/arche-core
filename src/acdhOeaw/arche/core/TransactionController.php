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
use Throwable;
use Socket;
use zozlak\logging\Log;
use acdhOeaw\arche\core\RepoException;
use acdhOeaw\arche\lib\Config;
use acdhOeaw\arche\core\Transaction as T;

/**
 * Description of TransactionController
 *
 * @author zozlak
 */
class TransactionController {

    const TYPE_UNIX = 'unix';
    const TYPE_INET = 'inet';

    /**
     * 
     * @param Config $config
     * @return array<mixed>
     * @throws RepoException
     */
    private static function getSocketConfig(Config $config): array {
        $c = $config->transactionController->socket;
        switch ($c->type) {
            case self::TYPE_INET:
                $type    = AF_INET;
                $address = $c->address;
                $port    = $c->port;
                break;
            case self::TYPE_UNIX:
                $type    = AF_UNIX;
                $address = $c->path;
                $port    = 0;
                break;
            default:
                throw new RepoException('Unknown socket type');
        }
        return [$type, $address, $port];
    }

    /**
     * Registers a new transaction by connecting to the transaction controller daemon
     * @param Config $config
     * @return int
     * @throws RepoException
     */
    public static function registerTransaction(Config $config): int {
        list($type, $address, $port) = self::getSocketConfig($config);

        $socket = @socket_create($type, SOCK_STREAM, 0);
        if ($socket === false) {
            throw new RepoException("Failed to create a socket: " . socket_strerror(socket_last_error()) . "\n");
        }

        $ret = @socket_connect($socket, $address, $port);
        if ($ret === false) {
            throw new RepoException("Failed to connect to a socket: " . socket_strerror(socket_last_error($socket)) . "\n");
        }

        $txId = socket_read($socket, 100, PHP_NORMAL_READ);

        socket_close($socket);
        return (int) $txId;
    }

    private string $configFile;
    private Config $config;
    private Socket $socket;
    private Log $log;
    private bool $loop  = true;
    private bool $child = false;

    public function __construct(string $configFile) {
        $this->configFile = $configFile;
        $this->loadConfig();
        $c                = $this->config->transactionController;

        $this->log = new Log($c->logging->file, $c->logging->level);

        list($type, $address, $port) = self::getSocketConfig($this->config);
        if (file_exists($address)) {
            unlink($address);
        }

        $socket = @socket_create($type, SOCK_STREAM, 0);
        if ($socket === false) {
            throw new RepoException("Failed to create a socket: " . socket_strerror(socket_last_error()) . "\n");
        }
        $this->socket = $socket;

        $ret = @socket_bind($this->socket, $address, $port);
        if ($ret === false) {
            throw new RepoException("Failed to bind to a socket: " . socket_strerror(socket_last_error($this->socket)) . "\n");
        }
        $ret = @socket_listen($this->socket, SOMAXCONN);
        if ($ret === false) {
            throw new RepoException("Failed to listen on a socket: " . socket_strerror(socket_last_error($this->socket)) . "\n");
        }
        $ret = socket_set_nonblock($this->socket);
        if ($ret === false) {
            throw new RepoException("Failed to set socket in a non-blocking mode\n");
        }
    }

    public function __destruct() {
        if (!$this->child) {
            if ($this->socket instanceof Socket) {
                socket_close($this->socket);
            }
            $c = $this->config->transactionController;
            if ($c->socket->type === self::TYPE_UNIX && file_exists($c->socket->path)) {
                unlink($c->socket->path);
            }
        }
    }

    public function handleRequests(): void {
        $status = null;
        while ($this->loop) {
            while (pcntl_waitpid(-1, $status, WNOHANG) > 0); // take care of zombies

            $connSocket = socket_accept($this->socket);
            if ($connSocket === false) {
                usleep(1000);
            } else {
                $pid = pcntl_fork();
                if ($pid === 0) {
                    $this->child = true;
                    $this->handleRequest($connSocket);
                    socket_close($connSocket);
                    $this->stop();
                } elseif ($pid === -1) {
                    $this->log->error("Failed to fork\n");
                    socket_close($connSocket);
                } else {
                    socket_close($connSocket);
                }
            }
        }
        $this->log->info('Waiting for child processes to exit');
        while (pcntl_waitpid(-1, $status) >= 0); // take care of zombies
        $this->log->info('Exiting');
    }

    public function stop(): void {
        $this->loop = false;
    }

    public function loadConfig(): void {
        if (isset($this->log)) {
            $this->log->info('Reloading configuration');
        }
        $this->config           = Config::fromYaml($this->configFile);
        RestController::$config = $this->config;
    }

    /**
     * 
     * @param Socket $connSocket
     * @return void
     */
    private function handleRequest(Socket $connSocket): void {
        try {
            $timeout       = $this->config->transactionController->timeout;
            $checkInterval = 1000 * $this->config->transactionController->checkInterval;

            $this->log->info("Handling a connection");

            $connStr    = $this->config->dbConn->admin;
            $pdo        = new PDO($connStr);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $preTxState = new PDO($connStr);
            $preTxState->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

            $pdo->query("SET application_name TO tx_controller");
            $preTxState->query("SET application_name TO txcontrollerpre");
            $preTxState->query("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE");
            $snapshot = $preTxState->query("SELECT pg_export_snapshot()");
            if ($snapshot !== false) {
                $snapshot = $snapshot->fetchColumn();
            }

            $query = $pdo->prepare("
                INSERT INTO transactions (transaction_id, snapshot) VALUES ((random() * 9223372036854775807)::bigint, ?) 
                RETURNING transaction_id AS id
            ");
            $query->execute([$snapshot]);
            $txId  = (int) $query->fetchColumn();
            $this->log->info("Transaction $txId created");

            $checkQuery        = $pdo->prepare("
                SELECT 
                    state, 
                    extract(epoch from now() - last_request) AS delay 
                FROM transactions 
                WHERE transaction_id = ?
                FOR UPDATE NOWAIT
            ");
            $checkQuery->execute([$txId]); // only to make sure it's runs fine before we confirm readiness to the client
            $checkResLockQuery = $pdo->prepare("
                SELECT count(*)
                FROM resources
                WHERE
                    transaction_id = ?
                    AND lock IS NOT NULL
            ");

            $ret = @socket_write($connSocket, $txId . "\n");
            if ($ret === false) {
                $this->log->error("Transaction $txId - client notification error: " . socket_strerror(socket_last_error($connSocket)));
            } else {
                $this->log->info("Transaction $txId - client notified");
            }
            $state = $this->makeState(T::STATE_ACTIVE, 0);
            while (true) {
                $this->logState($txId, $state); // out of the transaction to minimize critical section
                usleep($checkInterval);

                $pdo->beginTransaction();
                try {
                    $checkQuery->execute([$txId]);
                    $state = $checkQuery->fetchObject();
                    $state = $state ?: $this->makeState(T::STATE_NOTX);

                    $checkResLockQuery->execute([$txId]);
                    $state->lockedResCount = $checkResLockQuery->fetchColumn();
                } catch (PDOException $e) {
                    $state = $e->getCode() === T::PG_LOCK_FAILURE ? T::STATE_LOCKED : T::STATE_NOTX;
                    $state = $this->makeState($state);
                }
                $timeoutCond = $state->state === T::STATE_ACTIVE && ($state->delay ?? 0) >= $timeout && ($state->lockedResCount ?? 0) === 0;
                $stateCond   = $state->state !== T::STATE_ACTIVE && $state->state !== T::STATE_LOCKED;
                if ($timeoutCond || $stateCond) {
                    break;
                }
                $pdo->commit();
            };
            $this->logState($txId, $state);

            if ($state->state !== T::STATE_COMMIT) {
                $query = $pdo->prepare("UPDATE transactions SET state = ? WHERE transaction_id = ?");
                $query->execute([T::STATE_ROLLBACK, $txId]);
            }
            $pdo->commit();

            if ($state->state === T::STATE_COMMIT) {
                $this->commitTransaction($txId, $pdo, $preTxState);
            } else {
                $this->rollbackTransaction($txId, $pdo, $preTxState);
            }
            $preTxState->query('COMMIT');

            $pdo->beginTransaction();
            $query = $pdo->prepare("UPDATE resources SET transaction_id = null, lock = null WHERE transaction_id = ?");
            $query->execute([$txId]);
            $query = $pdo->prepare("DELETE FROM transactions WHERE transaction_id = ?");
            $query->execute([$txId]);
            $pdo->commit();
        } catch (Throwable $e) {
            $this->log->error($e);
        } finally {
            if (isset($pdo) && isset($txId)) {
                if ($pdo->inTransaction()) {
                    $pdo->rollBack();
                }
                $pdo->beginTransaction();
                $query = $pdo->prepare("UPDATE resources SET transaction_id = null, lock = null WHERE transaction_id = ?");
                $query->execute([$txId]);
                $query = $pdo->prepare("DELETE FROM transactions WHERE transaction_id = ?");
                $query->execute([$txId]);
                $pdo->commit();
            }
            $txId = $txId ?? '';
            $this->log->info("Transaction $txId finished");
        }
    }

    /**
     * Rolls back a transaction by:
     * - finding all resources visible for the $currState assigned to the transaction $txId
     * - bringing their state back to the one visible for the $preTxState
     * @param int $txId
     * @param PDO $curState
     * @param PDO $prevState
     * @return void
     */
    private function rollbackTransaction(int $txId, PDO $curState,
                                         PDO $prevState): void {
        $this->log->info("Transaction $txId - rollback");

        $queryResDelCheck = $curState->prepare("
            SELECT transaction_id
            FROM
                resources
                JOIN relations USING (id)
            WHERE
                target_id = ?
                AND transaction_id <> ?
            LIMIT 1
        ");
        $queryResDel      = $curState->prepare("DELETE FROM resources r WHERE id = ?");
        $queryResMigrate  = $curState->prepare("UPDATE resources SET transaction_id = ? WHERE id = ?");
        $queryIdDel       = $curState->prepare("DELETE FROM identifiers WHERE id = ?");
        $queryRelDel      = $curState->prepare("DELETE FROM relations WHERE id = ?");
        $queryMetaDel     = $curState->prepare("DELETE FROM metadata WHERE id = ?");
        $queryFtsDel      = $curState->prepare("DELETE FROM full_text_search WHERE id = ?");
        $queryResUpd      = $curState->prepare("UPDATE resources SET state = ?, transaction_id = null, lock = null WHERE id = ?");
        $queryIdIns       = $curState->prepare("INSERT INTO identifiers (ids, id) VALUES (?, ?)");
        $queryRelIns      = $curState->prepare("INSERT INTO relations (id, target_id, property) VALUES (?, ?, ?)");
        $queryMetaIns     = $curState->prepare("INSERT INTO metadata (mid, id, property, type, lang, value_n, value_t, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        $queryFtsIns      = $curState->prepare("INSERT INTO full_text_search (ftsid, id, segments, raw) VALUES (?, ?, ?, ?)");
        $queryIdSel       = $prevState->prepare("SELECT ids, id FROM identifiers WHERE id = ?");
        $queryRelSel      = $prevState->prepare("SELECT id, target_id, property FROM relations WHERE id = ?");
        $queryMetaSel     = $prevState->prepare("SELECT mid, id, property, type, lang, value_n, value_t, value FROM metadata WHERE id = ?");
        $queryFtsSel      = $prevState->prepare("SELECT ftsid, id, segments, raw FROM full_text_search WHERE id = ?");
        $queryPrev        = $prevState->prepare("SELECT state FROM resources WHERE id = ?");
        // order by state assures 'active' come first which allows avoiding identifiers conflict on restoring ids of deleted resources
        $queryCur         = $curState->prepare("SELECT id FROM resources WHERE transaction_id = ? ORDER BY state");
        $queryCur->execute([$txId]);
        $toRestore        = [];
        // deferred foreign key on relations.target_id won't work without a transaction
        $curState->beginTransaction();
        $curState->query("CREATE TEMPORARY TABLE _removed_ids AS SELECT * FROM identifiers LIMIT 0");
        while ($rid              = (int) $queryCur->fetchColumn()) {
            $queryPrev->execute([$rid]);
            $state = $queryPrev->fetchColumn();
            if ($state === false) {
                // resource didn't exist before - delete it but only if it's not referenced
                // by a resource from other transaction (or no transaction at all)
                // if it's referenced, keep it but mark it as belonging to the referer transaction
                $this->log->debug("  deleting $rid ($txId)");
                $queryResDelCheck->execute([$rid, $txId]);
                $otherTx = $queryResDelCheck->fetchColumn();
                if ($otherTx === false) {
                    $queryResDel->execute([$rid]);
                    $binary = new BinaryPayload($rid);
                    $binary->delete();
                } else {
                    $this->log->debug("    keeping $rid ($txId) and migrating to transaction " . ($otherTx ?? 'null'));
                    $queryResMigrate->execute([$otherTx, $rid]);
                }
            } else {
                // must be processed later as they can cause conflicts with resources still to be deleted
                $toRestore[$rid] = $state;
            }
        }
        foreach ($toRestore as $rid => $state) {
            // resource existed before - restore it's state
            $this->log->debug("  restoring $rid state to $state ($txId)");

            $queryIdDel->execute([$rid]);
            $queryIdSel->execute([$rid]);
            while ($i = $queryIdSel->fetch(PDO::FETCH_NUM)) {
                $queryIdIns->execute($i);
            }

            $queryRelDel->execute([$rid]);
            $queryRelSel->execute([$rid]);
            while ($i = $queryRelSel->fetch(PDO::FETCH_NUM)) {
                $queryRelIns->execute($i);
            }

            $queryMetaDel->execute([$rid]);
            $queryMetaSel->execute([$rid]);
            while ($i = $queryMetaSel->fetch(PDO::FETCH_NUM)) {
                $queryMetaIns->execute($i);
            }

            $queryFtsDel->execute([$rid]);
            $queryFtsSel->execute([$rid]);
            while ($i = $queryFtsSel->fetch(PDO::FETCH_NUM)) {
                $queryFtsIns->execute($i);
            }

            $binary = new BinaryPayload($rid);
            $ret    = $binary->restore((string) $txId);
            if ($ret) {
                $this->log->debug("    binary state of $rid restored ($txId)");
            }
            
            // at the end as it clears the transaction_id of a resource
            $queryResUpd->execute([$state, $rid]);
        }
        $curState->commit();
    }

    /**
     * Commits a transaction, e.g. saves metadata history changes.
     * @param int $txId
     * @param PDO $curState
     * @param PDO $prevState
     * @return void
     */
    private function commitTransaction(int $txId, PDO $curState, PDO $prevState): void {
        $this->log->info("Transaction $txId - commit");

        if ($this->config->transactionController->simplifyMetaHistory) {
            $query = $curState->prepare("
                WITH todel AS (
                    SELECT *
                    FROM (
                        SELECT 
                            mh.*, 
                            min(date) OVER (PARTITION BY id) AS datemin
                        FROM
                            metadata_history mh
                            JOIN resources r USING (id)
                            JOIN transactions t USING (transaction_id)
                        WHERE 
                            transaction_id = ?
                            AND mh.date >= t.started
                    ) t1
                    WHERE date > datemin
                )
                DELETE FROM metadata_history WHERE midh IN (SELECT midh FROM todel)
            ");
            $query->execute([$txId]);
            $this->log->info("Transaction $txId - " . $query->rowCount() . " metadata history rows removed");
        }
    }

    /**
     * 
     * @param string $state
     * @param int|null $delay
     * @param int|null $lockedResCount
     * @return object
     */
    private function makeState(string $state, ?int $delay = null,
                               ?int $lockedResCount = null): object {
        return (object) [
                'state'          => $state,
                'delay'          => $delay,
                'lockedResCount' => $lockedResCount,
        ];
    }

    /**
     * 
     * @param object $state
     * @return void
     */
    private function logState(int $txId, object $state): void {
        $this->log->debug("Transaction $txId state: $state->state, " . ($state->delay ?? '?') . " s " . ($state->lockedResCount ?? '?') . " locked resources");
    }
}
