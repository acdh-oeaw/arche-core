#!/usr/bin/php
<?php

/*
 * The MIT License
 *
 * Copyright 2020 Austrian Centre for Digital Humanities.
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

$composerDir = realpath(__DIR__);
while ($composerDir !== false && !file_exists("$composerDir/vendor")) {
    $composerDir = realpath("$composerDir/..");
}
require_once "$composerDir/vendor/autoload.php";

use splitbrain\PHPArchive\Tar;
use zozlak\logging\Log;
use Psr\Log\LogLevel;

class BackupException extends Exception {
    
}

$params = [
    'compression'      => 'none',
    'compressionLevel' => 3,
    'lock'             => 'wait',
    'chunkSize'        => 0,
    'include'          => 'all',
    'checkOnly'        => false,
    'help'             => false,
];
$n      = 0;
$argv ??= [];
for ($i = 1; $i < count($argv); $i++) {
    if ($argv[$i] === '--help') {
        $params['help'] = true;
    } elseif ($argv[$i] === '--checkOnly') {
        $params['checkOnly'] = true;
    } elseif (substr($argv[$i], 0, 2) === '--') {
        $params[substr($argv[$i], 2)] = $argv[$i + 1];
        $i++;
    } else {
        $params[$n] = $argv[$i];
        $n++;
    }
}

$targetFile = $params[0] ?? '';
if ($params['help'] || empty($targetFile) || empty($params[1] ?? '') && !$params['checkOnly']) {
    exit(<<<AAA
backup.php [--dateFile path] [--dateFrom yyyy-mm-ddThh:mm:ss] [--dateTo yyyy-mm-ddThh:mm:ss] [--compression method] [--compressionLevel level] [--include mode] [--lock mode] [--chunkSize sizeMiB] [--dbConn connectionName] [--tmpDir tmpDir] [--checkOnly] targetFile repoConfigFile

Creates a repository backup.

Parameters:
    targetFile path of the created dump file
    repoConfigFile path to the repository config yaml file
    
    --dateFile path to a file storying the --dateFrom parameter value
        the file content is automatically updated with the --dateTo value after a successful dump
        which provides an easy implementation of incremental backups
        if the file doesn't exist, 1900-01-01T00:00:00 is assumed as the --dateFrom value
        --dateFrom takes precedence over --dateFile content
    --dateFrom, --dateTo only binaries modified within a given time period are included in the dump
        (--dateFrom default is 1900-01-01T00:00:00, --dateTo default is current date and time)
        --dateFrom takes precedence over --dateFile
    --compression (default none) compression method - one of none/bzip/gzip
    --compressionLevel (default 3) compression level from 1 to 9
    --include (default all) set of database tables to include:
        none - don't include a database dump
        all - include all tables
        skipSearch - skip full text search and spatial tables
        skipHistory - skip metadata history table
        skipSearchHistory - skip both full text search, spatial search and metadata history table
    --lock (default wait) - how to aquire a databse lock to assure dump consistency?
        try - try to acquire a lock on all matching binaries and fail if it's not possible
        wait - wait until it's possible to acquire a lock on all matching binaries
        skip - acquire lock on all matching binaries which are not cuurently locked by other transactions
    --chunkSize maximum size of resources included in a single targetFile.
        After a resource added to the targetFile exceeds this size a new targetFile with a name suffix 
        is created.
        If compression is used, the targetSize files can be smaller than this size.
    --dbConn (default backup) name of the database connection parameters in the config file
    --tmpDir (default read from the yaml config) temporary directory location. For performance reasons
        you might prefer to set it to the same directory as the targetFile.
    --checkOnly if set, the targetFile consistency is checked (hashes of files it contains are checked
        against hashes in the corresponding .list file) instead of a backup file creation


AAA
    );
}

$log = new Log('php://stdout', LogLevel::DEBUG);
try {
    if (substr($targetFile, 0, 1) !== '/') {
        $targetFile = getcwd() . '/' . $targetFile;
    }
    $targetFile = preg_replace('`[.][^./]+$`', '', $targetFile);
    if (!file_exists(dirname($targetFile)) || !is_dir(dirname($targetFile))) {
        throw new Exception("Target location '" . dirname($targetFile) . "' does not exist or is not a directory");
    }
    if ($params['checkOnly']) {
        goto CHECK;
    }

    // CONFIG PARSING
    if (!file_exists($params[1])) {
        print_r($params);
        throw new Exception('Repository config yaml file does not exist');
    }
    $cfg = yaml_parse_file($params[1]);
    if ($cfg === false) {
        throw new Exception('Repository config yaml file can not be parsed as YAML');
    }
    $cfg = json_decode(json_encode($cfg));

    if (substr($cfg->storage->dir, 0, 1) !== '/') {
        throw new Exception('Storage dir set up as a relative path in the repository config file - can not determine paths');
    }

    $tmpDir = $params['tmpDir'] ?? $cfg->storage->tmpDir;
    if (!file_exists($tmpDir) || !is_dir($tmpDir)) {
        throw new Exception("Temporary directory '$tmpDir' does not exist or is not a directory");
    }
    $tmpFileBase = $tmpDir . '/' . basename($targetFile);

    $pgdumpConnParam = ['host' => '-h', 'port' => '-p', 'dbname' => '', 'user' => '-U'];
    $connName        = $params['dbConn'] ?? 'backup';
    $pdoConnStr      = $cfg->dbConnStr->$connName ?? 'pgsql:';
    $pgdumpConnStr   = 'pg_dump';
    foreach (explode(' ', preg_replace('/ +/', ' ', trim(substr($pdoConnStr, 6)))) as $i) {
        if (!empty($i)) {
            $k = substr($i, 0, strpos($i, '='));
            $v = substr($i, 1 + strpos($i, '='));
            if (isset($pgdumpConnParam[$k])) {
                $pgdumpConnStr .= ' ' . $pgdumpConnParam[$k] . " '" . $v . "'";
            } elseif ($v === 'password') {
                $pgdumpConnStr = "PGPASSWORD='$v' " . $pgdumpConnStr;
            } else {
                throw new Exception("Unknown database connection parameter: $k");
            }
        }
    }

    if (isset($params['dateFile'])) {
        $params['dateFile'] = realpath(dirname($params['dateFile'])) . '/' . basename($params['dateFile']);
        if (!isset($params['dateFrom']) && file_exists($params['dateFile'])) {
            $params['dateFrom'] = trim(file_get_contents($params['dateFile']));
        }
    }
    $params['dateFrom'] = $params['dateFrom'] ?? '1900-01-01 00:00:00';
    $params['dateTo']   = $params['dateTo'] ?? date('Y-m-d H:i:s');

    $log->info("Dumping binaries for time period " . $params['dateFrom'] . " - " . $params['dateTo']);

    // BEGINNING TRANSACTION
    $log->info("Acquiring database locks");

    try {
        $pdo = new PDO($pdoConnStr);
    } catch (PDOException) {
        throw new BackupException("Could not connect to the database using the settings '$pdoConnStr'");
    }
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->query("SET application_name TO backupscript");
    $pdo->beginTransaction();
    $query = $pdo->prepare("
        INSERT INTO transactions (transaction_id, snapshot) 
        SELECT coalesce(max(transaction_id), 0) + 1, 'backup tx' FROM transactions 
        RETURNING transaction_id
    ");
    $query->execute();
    $txId  = $query->fetchColumn();

    $matchQuery = "
        SELECT id
        FROM 
            resources
            JOIN metadata m1 USING (id)
            JOIN metadata m2 USING (id)
        WHERE 
                m1.property = ? AND m1.value_t BETWEEN ? AND ?
            AND m2.property = ? AND m2.value_n > 0
    ";
    $matchParam = [
        $cfg->schema->binaryModificationDate,
        $params['dateFrom'],
        $params['dateTo'],
        $cfg->schema->binarySize,
    ];

    $matchQuery .= match ($params['lock']) {
        'try' => " FOR UPDATE NOWAIT",
        'wait' => " FOR UPDATE",
        'skip' => " FOR UPDATE SKIP LOCKED",
        default => throw new BackupException("Unknown lock method '" . $params['lock'] . "' - should be one of try/wait/skip"),
    };
    $matchQuery = $pdo->prepare("
        UPDATE resources SET transaction_id = ? WHERE id IN ($matchQuery)
    ");
    try {
        $matchQuery->execute(array_merge([$txId], $matchParam));
    } catch (PDOException $e) {
        if ($e->getCode() === '55P03') {
            $pdo->rollBack();
            throw new BackupException('Some matching binaries locked by other transactions');
        }
        throw $e;
    }
    $snapshot = $pdo->query("SELECT pg_export_snapshot()")->fetchColumn();

    // DATABASE
    if ($params['include'] !== 'none') {
        $sqlFile   = $tmpFileBase . '.sql';
        $dbDumpCmd = "$pgdumpConnStr -a -T *_seq -T transactions --snapshot $snapshot -f $sqlFile";
        $dbDumpCmd .= ($params['include']) == 'skipSearch' ? ' -T full_text_search -T spatial_search' : '';
        $dbDumpCmd .= ($params['include']) == 'skipHistory' ? ' -T metadata_history' : '';
        $dbDumpCmd .= ($params['include']) == 'skipSearchHistory' ? ' -T full_text_search -T metadata_history' : '';
        $log->info("Dumping database with: $dbDumpCmd");
        $out       = $ret       = null;
        exec($dbDumpCmd, $out, $ret);
        if ($ret !== 0) {
            throw new Exception("Dumping database failed:\n\n" . implode("\n", $out));
        }
        $log->info(sprintf("    dump size: %.3f MB\n", filesize($sqlFile) / 1024 / 1024));
        $pdo->commit(); // must be here so the snapshot passed to pg_dump exists
    }
    // BINARIES
    $log->info("Selecting binaries");
    $query = $pdo->prepare("SELECT count(*), sum(value_n) FROM resources JOIN metadata USING (id) WHERE transaction_id = ? AND property = ?");
    $query->execute([$txId, $cfg->schema->binarySize]);
    list($totalN, $totalSize) = $query->fetch(PDO::FETCH_NUM);
    $log->info("    found $totalN file(s) with a total size of " . round($totalSize >> 20, 3) . " MB");

    function getStorageDir(int $id, string $path, int $level, int $levelMax): string {
        if ($level < $levelMax) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            $path = getStorageDir((int) $id / 100, $path, $level + 1, $levelMax);
        }
        return $path;
    }

    function renameTmp(string $targetFile, int $part, string $tmpArchivePath,
                       string $tmpListPath, string $compression): void {
        global $log;
        $log->info("    closing chunk $part");
        $ext  = match ($compression) {
            'bzip' => '.tbz',
            'gzip' => '.tgz',
            default => '.tar',
        };
        $part = $part > 0 ? "_part$part" : "";
        rename($tmpArchivePath, "$targetFile$part$ext");
        rename($tmpListPath, "$targetFile$part.list");
        $log->info("        ended");
    }

    /**
     * @param array<string, mixed> $params
     */
    function createArchive(string $path, array $params): Tar {
        $tar         = new Tar();
        $compression = match ($params['compression']) {
            'bzip' => Tar::COMPRESS_BZIP,
            'gzip' => Tar::COMPRESS_GZIP,
            default => null,
        };
        if (!empty($compression)) {
            $tar->setCompression($params['compressionLevel'], $compression);
        }
        $tar->create($path);
        return $tar;
    }
    $chunksCount    = 0;
    $chunkMaxSize   = ($params['chunkSize'] << 20) ?: PHP_INT_MAX;
    $pathCutpoint   = strlen(preg_replace('|/$|', '', $cfg->storage->dir)) + 1;
    $tmpArchivePath = $tmpFileBase . '.archive';
    $tmpListPath    = $tmpFileBase . '.list';
    $tmpArchive     = createArchive($tmpArchivePath, $params);
    $tmpList        = fopen($tmpListPath, 'w');
    if ($tmpList === false) {
        throw new Exception("Failed to open files list file '$tmpListPath' for writing");
    }

    $query  = $pdo->prepare("SELECT id, value AS hash FROM resources JOIN metadata USING (id) WHERE transaction_id = ? AND property = ?");
    $query->execute([$txId, $cfg->schema->hash]);
    $size   = $SIZE   = 0;
    $n      = $N      = 0;
    $t      = $tStart = time();
    $log->info("Creating backup file(s) using $tmpArchivePath temp file");
    while ($res    = $query->fetch(PDO::FETCH_OBJ)) {
        $path = getStorageDir($res->id, $cfg->storage->dir, 0, $cfg->storage->levels) . '/' . $res->id;
        if (!file_exists($path)) {
            $log->error("binary $path is missing");
            continue;
        }
        $fSize = filesize($path);
        $lPath = substr($path, $pathCutpoint);
        fwrite($tmpList, "$lPath,$res->hash\n");
        $tmpArchive->addFile($path, $lPath);
        $size  += $fSize;
        $SIZE  += $fSize;
        $n++;
        $N++;
        if (time() - $t > 10) {
            $t = time();
            $log->debug("~   $N / $totalN (" . round(100 * $N / $totalN, 2) . "%) " . ($SIZE >> 20) . "MB / " . ($totalSize >> 20) . "MB (" . round(100 * $SIZE / $totalSize, 2) . "%) " . round($t - $tStart) . " s");
        }
        if ($size > $chunkMaxSize) {
            $tmpArchive->close();
            fclose($tmpList);
            $chunksCount++;
            renameTmp($targetFile, $chunksCount, $tmpArchivePath, $tmpListPath, $params['compression']);
            $tmpArchive = createArchive($tmpArchivePath, $params);
            $tmpList    = fopen($tmpListPath, 'w');
            if ($tmpList === false) {
                throw new Exception("Failed to open files list file '$tmpListPath' for writing");
            }
            $size = 0;
            $n    = 0;
        }
    }
    if (isset($sqlFile) && file_exists($sqlFile)) {
        $log->info("    adding SQL dump file");
        $n++;
        $N++;
        fwrite($tmpList, "dbdump.sql,sha1:" . sha1_file($sqlFile) . "\n");
        $tmpArchive->addFile($sqlFile, 'dbdump.sql');
    }
    $tmpArchive->close();
    fclose($tmpList);
    if ($n > 0 || $N === 0) {
        $chunksCount += $chunksCount > 0;
        renameTmp($targetFile, $chunksCount, $tmpArchivePath, $tmpListPath, $params['compression']);
    }

    // CHECKING HASHES
    CHECK:
    $targetFiles = array_merge(
        glob($targetFile . "*tar"),
        glob($targetFile . "*tgz"),
        glob($targetFile . "*tbz"),
    );
    if (count($targetFiles) === 0) {
        $log->warning("No backup files to check");
    }
    foreach ($targetFiles as $i) {
        $log->info("Checking content of $i");
        $listFile = dirname($i) . '/' . preg_replace('/[^.]+$/', 'list', basename($i));
        if (!file_exists($listFile)) {
            $log->error("list file for $i not found");
            continue;
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
        $dataFile->open($i);
        $t        = $tStart   = time();
        $n        = 0;
        foreach ($dataFile->yieldContents() as $i) {
            if ($i->getIsdir()) {
                continue;
            }
            $n++;
            $path = $i->getPath();
            if (!isset($ref[$path])) {
                $log->error("$path not in the list file");
                continue;
            }
            list($algo, $refhash) = explode(':', $ref[$path]);
            $hash  = hash_init($algo);
            while ($chunk = $dataFile->readCurrentEntry(4194304)) {
                hash_update($hash, $chunk);
            }
            $hash = hash_final($hash);
            if ($refhash !== $hash) {
                $log->error("$path hash $hash does not match hash from the list file $refhash");
                $exit = 2;
            }
            unset($ref[$path]);
            if (time() - $t > 10) {
                $t = time();
                $log->debug("~   $n / $N (" . round(100 * $n / $N, 2) . "%) " . round($t - $tStart) . " s");
            }
        }
        foreach ($ref as $path) {
            $log->error("$path not in the backup");
            $exit = 2;
        }
    }

    // FINISHING
    if (isset($params['dateFile']) && !isset($exit)) {
        $log->info("Updating date file '" . $params['dateFile'] . "' with " . $params['dateTo']);
        file_put_contents($params['dateFile'], $params['dateTo']);
    }
    $log->info("Dump completed " . (isset($exit) ? 'wit errors' : 'successfully'));
} catch (BackupException $e) {
    // Well-known errors which don't require stack traces
    foreach ($targetFiles ?? [] as $i) {
        if (file_exists($i)) {
            unlink($i);
        }
    }
    $log->error($e->getMessage());
    $exit = 1;
} catch (Throwable $e) {
    throw $e;
} finally {
    if (is_resource($tmpList ?? null)) {
        fclose($tmpList);
    }
    if (isset($tmpArchive)) {
        unset($tmpArchive);
    }
    $tmpFileBase ??= "/__no such dir__/";
    foreach (array_merge([$sqlFile ?? ''], glob("$tmpFileBase*")) as $i) {
        if (file_exists($i)) {
            unlink($i);
        }
    }
    if (isset($pdo) && !empty($txId)) {
        $log->info("Releasing database locks");
        $query = $pdo->prepare("UPDATE resources SET transaction_id = NULL WHERE transaction_id = ?");
        $query->execute([$txId]);
        $query = $pdo->prepare("DELETE FROM transactions WHERE transaction_id = ?");
        $query->execute([$txId]);
    }
}
exit($exit ?? 0);
