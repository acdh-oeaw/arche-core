<?php

/*
 * The MIT License
 *
 * Copyright 2026 zozlak.
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
use DateTimeImmutable;
use DateInterval;
use Psr\Log\LoggerInterface;
use splitbrain\PHPArchive\Tar;
use acdhOeaw\arche\core\util\BackupParameters;

/**
 * Description of Backup
 *
 * @author zozlak
 */
class Backup {

    static public function formatMb(int $size, int $precision = 3): string {
        return round($size >> 20, $precision) . "MB";
    }

    const ERROR_NO_ERROR     = 0;
    const ERROR_BACKUP_ERROR = 1;
    const ERROR_HASHES_CHECK = 2;
    const DATE_FORMAT        = 'Y-m-d H:i:s';

    private string $dateFrom;
    private string $dateTo;
    private string $tmpFileBase = '/__no such dir__/';
    private string $sqlFile     = '';
    private string $pgdumpConnStr;
    private string $modDateProp;
    private string $sizeProp;
    private string $hashProp;
    private int $pathCutpoint;

    /**
     * 
     * @var callable
     */
    private $getPath;

    /**
     * 
     * @var open-resource
     */
    private $tmpList;
    private Tar $tmpArchive;
    private PDO $pdo;
    private int $txId;
    private string $snapshotId;
    private int $exitCode;

    public function __construct(private BackupParameters $params,
                                private LoggerInterface | null $log = null) {
        
    }

    public function run(): int {
        $this->exitCode = self::ERROR_NO_ERROR;

        try {
            $targetDir = dirname($this->params->targetFile);
            if (!file_exists($targetDir) || !is_dir($targetDir)) {
                throw new BackupException("Target location '$targetDir' does not exist or is not a directory");
            }
            if ($this->params->checkOnly) {
                $this->checkFiles($this->params->targetFile);
            } else {
                $this->processParams();
                $this->acquireDbLocks();
                if ($this->params->include !== BackupParameters::INCLUDE_NONE) {
                    $this->backupDb();
                }
                $this->pdo->commit();
                $this->processChunks();
                $this->log?->info("Dump completed " . ($this->exitCode === self::ERROR_NO_ERROR ? 'successfully' : 'with errors'));
            }
        } catch (BackupException $e) {
            // Well-known errors which don't require stack traces
            $this->log?->error($e->getMessage());
            $this->exitCode = self::ERROR_BACKUP_ERROR;
        } catch (Throwable $e) {
            throw $e;
        } finally {
            $this->cleanup();
        }
        return $this->exitCode;
    }

    public function checkFiles(string $targetFilePrefix): void {
        $targetFiles = array_merge(
            glob($targetFilePrefix . "*tar"),
            glob($targetFilePrefix . "*tgz"),
            glob($targetFilePrefix . "*tbz"),
        );
        if (count($targetFiles) === 0) {
            $this->log?->warning("No backup files to check");
        }
        foreach ($targetFiles as $i) {
            $this->checkFile($i);
        }
    }

    public function checkFile(string $file): void {
        $this->log?->info("Checking content of $file");
        $listFile = dirname($file) . '/' . pathinfo($file, PATHINFO_FILENAME) . '.list';
        if (!file_exists($listFile)) {
            $this->exitCode = self::ERROR_HASHES_CHECK;
            $this->log?->error("list file for $file not found");
            return;
        }
        // read reference hashes
        $ref      = [];
        $listFile = fopen($listFile, 'r');
        while (($l        = fgetcsv($listFile)) && count($l) === 2) {
            $ref[$l[0]] = $l[1];
        }
        fclose($listFile);
        $N        = count($ref);
        // read the file
        $dataFile = new Tar();
        $dataFile->open($file);
        $t        = $tStart   = time();
        $n        = 0;
        foreach ($dataFile->yieldContents() as $i) {
            if ($i->getIsdir()) {
                continue;
            }
            $n++;
            $path = $i->getPath();
            if (!isset($ref[$path])) {
                $this->log?->error("$path not in the list file");
                continue;
            }
            list($algo, $refhash) = explode(':', $ref[$path]);
            $hash  = hash_init($algo);
            while ($chunk = $dataFile->readCurrentEntry(4194304)) {
                hash_update($hash, $chunk);
            }
            $hash = hash_final($hash);
            if ($refhash !== $hash) {
                $this->log?->error("$path hash $hash does not match hash from the list file $refhash");
                $this->exitCode = self::ERROR_HASHES_CHECK;
            }
            unset($ref[$path]);
            if (time() - $t > 10) {
                $t = time();
                $this->log?->debug("~   $n / $N (" . round(100 * $n / $N, 2) . "%) " . round($t - $tStart) . " s");
            }
        }
        foreach ($ref as $path) {
            $this->log?->error("$path not in the backup");
            $this->exitCode = self::ERROR_HASHES_CHECK;
        }
    }

    private function processParams(): void {
        $configFile = $this->params->configFile;
        if (!file_exists($configFile)) {
            throw new BackupException("Repository config yaml file '$configFile' does not exist");
        }
        $cfg = yaml_parse_file($configFile);
        if ($cfg === false) {
            throw new BackupException("Repository config yaml file '$configFile' can not be parsed as YAML");
        }
        $cfg = json_decode((string) json_encode($cfg));

        if (substr($cfg->storage->dir, 0, 1) !== '/') {
            throw new BackupException('Storage dir set up as a relative path in the repository config file - can not determine paths');
        }

        $this->modDateProp  = $cfg->schema->binaryModificationDate;
        $this->sizeProp     = $cfg->schema->binarySize;
        $this->hashProp     = $cfg->schema->hash;
        $this->pathCutpoint = strlen(preg_replace('|/$|', '', $cfg->storage->dir)) + 1;
        $this->getPath      = fn($x) => $this->getStorageDir($x, $cfg->storage->dir, 0, $cfg->storage->levels) . '/' . $x;

        $tmpDir = $this->params->tmpDir ?? $cfg->storage->tmpDir;
        if (!file_exists($tmpDir) || !is_dir($tmpDir)) {
            throw new BackupException("Temporary directory '$tmpDir' does not exist or is not a directory");
        }
        $this->tmpFileBase = $tmpDir . '/' . basename($this->params->targetFile);

        $pgdumpConnParam     = ['host' => '-h', 'port' => '-p', 'dbname' => '', 'user' => '-U'];
        $connName            = empty($this->params->dbConn) ? 'backup' : $this->params->dbConn;
        $pdoConnStr          = $cfg->dbConnStr->$connName ?? 'pgsql:';
        $this->pgdumpConnStr = 'pg_dump';
        foreach (explode(' ', preg_replace('/ +/', ' ', trim(substr($pdoConnStr, 6)))) as $i) {
            if (!empty($i)) {
                $k = substr($i, 0, strpos($i, '='));
                $v = substr($i, 1 + strpos($i, '='));
                if (isset($pgdumpConnParam[$k])) {
                    $this->pgdumpConnStr .= ' ' . $pgdumpConnParam[$k] . " '" . $v . "'";
                } elseif ($k === 'password') {
                    $this->pgdumpConnStr = "PGPASSWORD='$v' " . $this->pgdumpConnStr;
                } else {
                    throw new BackupException("Unknown database connection parameter: $k");
                }
            }
        }
        try {
            $this->pdo = new PDO($pdoConnStr);
        } catch (PDOException) {
            throw new BackupException("Could not connect to the database using the settings '$pdoConnStr'");
        }
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->query("SET application_name TO backupscript");

        $dateFile = $this->params->dateFile;
        if (!empty($dateFile) && file_exists($dateFile)) {
            $this->dateFrom = trim(file_get_contents($dateFile));
        } elseif (!empty($this->params->dateFrom)) {
            $this->dateFrom = $this->params->dateFrom;
        } else {
            $this->dateFrom = '1900-01-01 00:00:00';
        }

        $this->dateTo = !empty($this->params->dateTo) ? $this->params->dateTo : date(self::DATE_FORMAT);

        $this->log?->info("Dumping binaries for time period " . $this->dateFrom . " - " . $this->dateTo);
    }

    private function acquireDbLocks(): void {
        $this->log?->info("Acquiring database locks");

        $backupsCount = $this->pdo->query("SELECT count(*) FROM transactions WHERE snapshot = 'backup tx'")->fetchColumn();
        if ($backupsCount && !$this->params->ifOtherBackup) {
            throw new BackupException('Other backup in progress');
        }

        $query      = $this->pdo->prepare("
            INSERT INTO transactions (transaction_id, snapshot) 
            SELECT coalesce(max(transaction_id), 0) + 1, 'backup tx' FROM transactions 
            RETURNING transaction_id
        ");
        $query->execute();
        $this->txId = $query->fetchColumn();

        $this->pdo->beginTransaction();
        $this->snapshotId = $this->pdo->query("SELECT pg_export_snapshot()")->fetchColumn();

        $matchQuery = "
            SELECT id
            FROM 
                resources
                JOIN metadata m1 USING (id)
                JOIN metadata m2 USING (id)
            WHERE 
                transaction_id IS NULL
                AND lock IS NULL
                AND m1.property = ? AND m1.value_t > ? AND m1.value_t <= ?
                AND m2.property = ? AND m2.value_n > 0
        ";
        $matchParam = [
            $this->modDateProp,
            $this->dateFrom,
            $this->dateTo,
            $this->sizeProp,
        ];

        $matchQuery .= match ($this->params->lock) {
            BackupParameters::LOCK_TRY => " FOR UPDATE NOWAIT",
            BackupParameters::LOCK_WAIT => " FOR UPDATE",
            BackupParameters::LOCK_SKIP => " FOR UPDATE SKIP LOCKED",
            default => throw new BackupException("Unknown lock method"),
        };
        $matchQuery = $this->pdo->prepare("
            UPDATE resources SET transaction_id = ? WHERE id IN ($matchQuery)
        ");
        try {
            $matchQuery->execute(array_merge([$this->txId], $matchParam));
        } catch (PDOException $e) {
            if ($e->getCode() === '55P03') {
                $this->pdo->rollBack();
                throw new BackupException('Some matching binaries locked by other transactions');
            }
            throw $e;
        }
    }

    private function backupDb(): void {
        $this->sqlFile = $this->tmpFileBase . '.sql';
        $dbDumpCmd     = $this->pgdumpConnStr . " -a -T *_seq -T transactions --snapshot " . $this->snapshotId . " -f " . $this->sqlFile;
        $include       = $this->params->include;
        if (in_array($include, [BackupParameters::INCLUDE_SKIP_SEARCH, BackupParameters::INCLUDE_SKIP_SEARCH_HISTORY])) {
            $dbDumpCmd .= ' -T full_text_search -T spatial_search';
        }
        if (in_array($include, [BackupParameters::INCLUDE_SKIP_HISTORY, BackupParameters::INCLUDE_SKIP_SEARCH_HISTORY])) {
            $dbDumpCmd .= ' -T metadata_history';
        }
        $this->log?->info("Dumping database with: $dbDumpCmd");
        $out = $ret = null;
        exec($dbDumpCmd, $out, $ret);
        if ($ret !== 0) {
            throw new BackupException("Dumping database failed:\n\n" . implode("\n", $out));
        }
        $this->log?->info("    dump size: " . self::formatMb(filesize($this->sqlFile)) . "\n");
        $this->pdo->commit(); // must be here so the snapshot passed to pg_dump exists
    }

    private function cleanup(): void {
        if (is_resource($this->tmpList)) {
            fclose($this->tmpList);
        }
        if (isset($this->tmpArchive)) {
            unset($this->tmpArchive);
        }
        foreach (glob($this->tmpFileBase . '*') as $i) {
            if (file_exists($i)) {
                unlink($i);
            }
        }
        if (isset($this->pdo) && !empty($this->txId)) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollback();
            }
            $this->log?->info("Releasing database locks");
            $query = $this->pdo->prepare("UPDATE resources SET transaction_id = NULL WHERE transaction_id = ?");
            $query->execute([$this->txId]);
            $query = $this->pdo->prepare("DELETE FROM transactions WHERE transaction_id = ?");
            $query->execute([$this->txId]);
        }
    }

    private function getStorageDir(int $id, string $path, int $level,
                                   int $levelMax): string {
        if ($level < $levelMax) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            $path = $this->getStorageDir((int) $id / 100, $path, $level + 1, $levelMax);
        }
        return $path;
    }

    private function renameTmp(string $targetFile, int $part,
                               string $tmpArchivePath, string $tmpListPath,
                               string $compression): string {
        global $log;
        $this->log?->info("    closing chunk $part");
        $ext        = match ($compression) {
            BackupParameters::COMPRESSION_BZIP => '.tbz',
            BackupParameters::COMPRESSION_GZIP => '.tgz',
            default => '.tar',
        };
        $part       = $part > 0 ? "_part$part" : "";
        $targetPath = "$targetFile$part$ext";
        rename($tmpArchivePath, $targetPath);
        rename($tmpListPath, "$targetFile$part.list");
        $this->log?->info("        ended");
        return $targetPath;
    }

    private function createArchive(string $path): Tar {
        if (file_exists($path)) {
            throw new BackupException("Target file '$path' already exists");
        }
        $tar         = new Tar();
        $compression = match ($this->params->compression) {
            BackupParameters::COMPRESSION_BZIP => Tar::COMPRESS_BZIP,
            BackupParameters::COMPRESSION_GZIP => Tar::COMPRESS_GZIP,
            default => null,
        };
        if (!empty($compression)) {
            $tar->setCompression($this->params->compressionLevel, $compression);
        }
        $tar->create($path);
        return $tar;
    }

    private function updateDateFile(string $date): void {
        $dateFile = $this->params->dateFile;
        if (empty($dateFile) || $this->exitCode !== self::ERROR_NO_ERROR || empty($date)) {
            return;
        }
        $this->log?->info("Updating date file '$dateFile' with " . $date);
        file_put_contents($dateFile, $date);
    }

    /**
     * 
     * @return array{0:int, 1:int}
     */
    private function getBackupSize(): array {
        $this->log?->info("Selecting binaries");
        $query     = $this->pdo->prepare("SELECT count(*), sum(value_n) FROM resources JOIN metadata USING (id) WHERE transaction_id = ? AND property = ?");
        $query->execute([$this->txId, $this->sizeProp]);
        list($totalN, $totalSize) = $query->fetch(PDO::FETCH_NUM);
        $totalSize ??= 0;
        $this->log?->info("    found $totalN file(s) with a total size of " . self::formatMb($totalSize));
        return [$totalN, $totalSize];
    }

    private function processChunks(): void {
        list($totalN, $totalSize) = $this->getBackupSize();
        $totalSizeMb = self::formatMb($totalSize);

        $chunkMaxSize   = ($this->params->chunkSize << 20) ?: PHP_INT_MAX;
        $tmpArchivePath = $this->tmpFileBase . '.archive';
        $tmpListPath    = $this->tmpFileBase . '.list';
        $tmpArchive     = $this->createArchive($tmpArchivePath);
        $tmpList        = fopen($tmpListPath, 'w');
        if ($tmpList === false) {
            throw new BackupException("Failed to open files list file '$tmpListPath' for writing");
        }

        $query  = $this->pdo->prepare("
            SELECT id, m1.value AS hash, m2.value AS moddate 
            FROM 
                resources 
                JOIN metadata m1 USING (id)
                JOIN metadata m2 USING (id)
            WHERE 
                transaction_id = ? 
                AND m1.property = ?
                AND m2.property = ?
            ORDER BY m2.value_t
        ");
        $query->execute([$this->txId, $this->hashProp, $this->modDateProp]);
        $chunk  = 0;
        $size   = $SIZE   = 0;
        $n      = $N      = 0;
        $t      = $tStart = time();
        $this->log?->info("Creating backup file(s) using $tmpArchivePath temp file");
        while ($this->exitCode === self::ERROR_NO_ERROR && ($res    = $query->fetch(PDO::FETCH_OBJ))) {
            $path = ($this->getPath)($res->id);
            if (!file_exists($path)) {
                $this->log?->error("binary $path is missing");
                continue;
            }

            $fSize = filesize($path);
            $lPath = substr($path, $this->pathCutpoint);
            fwrite($tmpList, "$lPath,$res->hash\n");
            $tmpArchive->addFile($path, $lPath);
            $size  += $fSize;
            $SIZE  += $fSize;
            $n++;
            $N++;
            if (time() - $t > 10) {
                $t = time();
                $this->log?->debug("~   $N / $totalN (" . round(100 * $N / $totalN, 2) . "%) " . self::formatMb($SIZE) . " / $totalSizeMb (" . round(100 * $SIZE / $totalSize, 2) . "%) " . round($t - $tStart) . " s");
            }
            if ($size > $chunkMaxSize) {
                $tmpArchive->close();
                fclose($tmpList);
                $chunk++;
                $targetPath = $this->renameTmp($this->params->targetFile, $chunk, $tmpArchivePath, $tmpListPath, $this->params->compression);
                $this->checkFile($targetPath);
                $this->updateDateFile($res->moddate);

                $tmpArchive = $this->createArchive($tmpArchivePath);
                $tmpList    = fopen($tmpListPath, 'w');
                if ($tmpList === false) {
                    throw new BackupException("Failed to open files list file '$tmpListPath' for writing");
                }
                $size = 0;
                $n    = 0;
            }
        }
        if (!empty($this->sqlFile) && file_exists($this->sqlFile)) {
            $this->log?->info("    adding SQL dump file");
            $n++;
            $N++;
            fwrite($tmpList, "dbdump.sql,sha1:" . sha1_file($this->sqlFile) . "\n");
            $tmpArchive->addFile($this->sqlFile, 'dbdump.sql');
        }
        $tmpArchive->close();
        fclose($tmpList);
        if ($n > 0 || $N === 0) {
            $chunk      += $chunk > 0;
            $targetPath = $this->renameTmp($this->params->targetFile, $chunk, $tmpArchivePath, $tmpListPath, $this->params->compression);
            $this->checkFile($targetPath);
            if (($res ?? false) !== false) {
                $this->updateDateFile($res->moddate);
            }
        }
    }
}
