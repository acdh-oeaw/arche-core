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

class BackupException extends Exception {
    
}

$params = [];
$n      = 0;
for ($i = 1; $i < count($argv); $i++) {
    if (substr($argv[$i], 0, 2) === '--') {
        $params[substr($argv[$i], 2)] = $argv[$i + 1];
        $i++;
    } else {
        $params[$n] = $argv[$i];
        $n++;
    }
}

$targetFile = $params[1] ?? '';
if (empty($targetFile) || empty($params[0] ?? '')) {
    exit(<<<AAA
backup.php [--dateFile path] [--dateFrom yyyy-mm-ddThh:mm:ss] [--dateTo yyyy-mm-ddThh:mm:ss] [--compression method] [--include mode] [--lock mode] repoConfigFile targetFile

Creates a repository backup.

Parameters:
    targetFile path of the created dump file
    repoConfigFile path to the repository config yaml file
    
    --dateFile path to a file storying the --dateFrom parameter value
        the file content is automatically updated with the --dateTo value after a successful dump
        whcich provides an easy implementation of incremental backups
        if the file doesn't exist, 1900-01-01T00:00:00 is assumed as the --dateFrom value
        --dateFrom takes precedence over --dateFile content
    --dateFrom, --dateTo only binaries modified within a given time period are included in the dump
        (--dateFrom default is 1900-01-01T00:00:00, --dateTo default is current date and time)
        --dateFrom takes precedence over --dateFile
    --compression (default none) compression method - one of none/bzip2/gzip
    --include (default all) set of database tables to include:
        all - include all tables
        skipSearch - skip full text search and spatial tables
        skipHistory - skip metadata history table
        skipSearchHistory - skip both full text search, spatial search and metadata history table
    --lock (default wait) - how to aquire a databse lock to assure dump consistency?
        try - try to acquire a lock on all matching binaries and fail if it's not possible
        wait - wait until it's possible to acquire a lock on all matching binaries
        skip - acquire lock on all matching binaries which are not cuurently locked by other transactions


AAA
    );
}

if (substr($targetFile, 0, 1) !== '/') {
    $targetFile = getcwd() . '/' . $targetFile;
}
try {
    // CONFIG PARSING
    if (!file_exists($params[0])) {
        print_r($params);
        throw new Exception('Repository config yaml file does not exist');
    }
    $cfg = yaml_parse_file($params[0]);
    if ($cfg === false) {
        throw new Exception('Repository config yaml file can not be parsed as YAML');
    }
    $cfg = json_decode(json_encode($cfg));

    if (substr($cfg->storage->dir, 0, 1) !== '/') {
        throw Exception('Storage dir set up as a relative path in the repository config file - can not determine paths');
    }

    $pgdumpConnParam = ['host' => '-h', 'port' => '-p', 'dbname' => '', 'user' => '-U'];
    $pdoConnStr      = $cfg->dbConnStr->backup ?? 'pgsql:';
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

    $targetFileSql  = $cfg->storage->dir . '/' . basename($targetFile) . '.sql';
    $targetFileList = $cfg->storage->tmpDir . '/' . basename($targetFile) . '.list';

    if (isset($params['dateFile'])) {
        $params['dateFile'] = realpath(dirname($params['dateFile'])) . '/' . basename($params['dateFile']);
        if (!isset($params['dateFrom']) && file_exists($params['dateFile'])) {
            $params['dateFrom'] = trim(file_get_contents($params['dateFile']));
        }
    }
    $params['dateFrom'] = $params['dateFrom'] ?? '1900-01-01 00:00:00';
    $params['dateTo']   = $params['dateTo'] ?? date('Y-m-d H:i:s');

    echo "Dumping binaries for time period " . $params['dateFrom'] . " - " . $params['dateTo'] . "\n";

    // BEGINNING TRANSACTION
    echo "Acquiring database locks\n";

    $pdo = new PDO($pdoConnStr);
    if ($pdo === false) {
        throw new Exception('Database connection failed.');
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

    switch ($params['lock'] ?? 'none') {
        case 'try':
            $matchQuery = $matchQuery . " FOR UPDATE NOWAIT";
            break;
        case 'wait':
            $matchQuery = $matchQuery . " FOR UPDATE";
            break;
        case 'skip':
            $matchQuery = $matchQuery . " FOR UPDATE SKIP LOCKED";
            break;
        default:
            throw new BackupException('Unknown lock method - should be one of try/wait/skip');
    }
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
    $dbDumpCmd = "$pgdumpConnStr -a -T *_seq -T transactions --snapshot $snapshot -f $targetFileSql";
    $dbDumpCmd .= ($params['include'] ?? '') == 'skipSearch' ? ' -T full_text_search -T spatial_search' : '';
    $dbDumpCmd .= ($params['include'] ?? '') == 'skipHistory' ? ' -T metadata_history' : '';
    $dbDumpCmd .= ($params['include'] ?? '') == 'skipSearchHistory' ? ' -T full_text_search -T metadata_history' : '';
    echo "Dumping database with:\n\t$dbDumpCmd\n";
    $out       = $ret       = null;
    exec($dbDumpCmd, $out, $ret);
    if ($ret !== 0) {
        throw new Exception("Dumping database failed:\n\n" . $out);
    }
    printf("\tdump size: %.3f MB\n", filesize($targetFileSql) / 1024 / 1024);
    $pdo->commit(); // must be here so the snapshot passed to pg_dump exists
    // BINARIES LIST FILE
    echo "Preparing binary files list\n";

    function getStorageDir(int $id, string $path, int $level, int $levelMax): string {
        if ($level < $levelMax) {
            $path = sprintf('%s/%02d', $path, $id % 100);
            $path = getStorageDir((int) $id / 100, $path, $level + 1, $levelMax);
        }
        return $path;
    }

    $query = $pdo->prepare("SELECT id FROM resources WHERE transaction_id = ?");
    $query->execute([$txId]);
    $tfl   = fopen($targetFileList, 'w');
    if ($tfl === false) {
        throw new Exception('Can not create binary files index file');
    }
    $nStrip = strlen(preg_replace('|/$|', '', $cfg->storage->dir)) + 1;
    $n      = $size   = 0;
    while ($id     = $query->fetchColumn()) {
        $path = getStorageDir($id, $cfg->storage->dir, 0, $cfg->storage->levels) . '/' . $id;
        if (file_exists($path)) {
            fwrite($tfl, substr($path, $nStrip) . "\n");
            $n++;
            $size += filesize($path);
        } else {
            echo "\twarning - binary $path is missing\n";
        }
    }
    $size = sprintf('%.3f', $size / 1024 / 1024);
    echo "\tfound $n file(s) with a total size of $size MB\n";

    fwrite($tfl, basename($targetFileSql) . "\n");
    fclose($tfl);
    $tfl = null;

    // OUTPUT FILE creation    
    chdir($cfg->storage->dir);
    $tarCmd = "tar -c -T $targetFileList";
    $tarCmd .= ($params['compression'] ?? '') === 'gzip' ? ' -z' : '';
    $tarCmd .= ($params['compression'] ?? '') === 'bzip2' ? ' -j' : '';
    $out    = $ret    = null;
    exec('pv -h', $out, $ret);
    $tarCmd .= $ret === 0 ? " | pv -F '        %b ellapsed: %t cur: %r avg: %a' > $targetFile" : "-f $targetFile";
    $tarCmd .= "; exit \${PIPESTATUS[0]}";
    echo "Creating dump with:\n\t$tarCmd\n";
    $tarCmd = 'bash -c ' . escapeshellarg($tarCmd); // bash is needed to use the $PIPESTATUS
    $ret    = null;
    system($tarCmd, $ret);
    if ($ret !== 0) {
        throw new Exception("Dump file creation failed");
    }

    // FINISHING
    if (isset($params['dateFile'])) {
        echo "Updating date file '" . $params['dateFile'] . "' with " . $params['dateTo'] . "\n";
        file_put_contents($params['dateFile'], $params['dateTo']);
    }
    echo "Dump completed successfully\n";
} catch (BackupException $e) {
    // Well-known errors which don't require stack traces
    if (file_exists($targetFile)) {
        unlink($targetFile);
    }
    echo 'ERROR: ' . $e->getMessage() . "\n";
    $exit = 1;
} catch (Throwable $e) {
    if (file_exists($targetFile)) {
        unlink($targetFile);
    }
    throw $e;
} finally {
    if ($tfl ?? null) {
        fclose($tfl);
    }
    foreach ([$targetFileSql, $targetFileList] as $f) {
        if (file_exists($f)) {
            unlink($f);
        }
    }
    if (isset($pdo) && !empty($txId)) {
        echo "Releasing database locks\n";
        $query = $pdo->prepare("UPDATE resources SET transaction_id = NULL WHERE transaction_id = ?");
        $query->execute([$txId]);
        $query = $pdo->prepare("DELETE FROM transactions WHERE transaction_id = ?");
        $query->execute([$txId]);
    }
}
exit($exit ?? 0);
