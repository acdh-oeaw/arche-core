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

namespace acdhOeaw\arche\core\util;

/**
 * Description of BackupArguments
 *
 * @author zozlak
 */
class BackupParameters {

    const HELP_STRING                 = <<<AAA
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
    --dateFrom, --dateTo only binaries modified within the <dateFrom, dateTo] time period are included in the dump
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
        skip - acquire lock on all matching binaries which are not cuurently locked by other database transactions
        It's worth noting that this applies only to Postgresql-level locks and not arche-core locks.
        In regard to arche-core locks (the resource.transaction_id column), only resources
        which are not locked by other arche-core transaction are selected.
    --ifOtherBackup - when set, the backup is made despite other backups being in progress;
        if not set and another backup is in progress, other backup attempt stops
    --chunkSize maximum size of resources included in a single targetFile in MB.
        After a resource added to the targetFile exceeds this size a new targetFile with a name suffix 
        is created.
        If compression is used, the targetSize files can be smaller than this size.
    --dbConn (default backup) name of the database connection parameters in the config file
    --tmpDir (default read from the yaml config) temporary directory location. For performance reasons
        you might prefer to set it to the same directory as the targetFile.
    --checkOnly if set, the targetFile consistency is checked (hashes of files it contains are checked
        against hashes in the corresponding .list file) instead of a backup file creation

AAA;
    const COMPRESSION_NONE            = 'none';
    const COMPRESSION_BZIP            = 'bzip';
    const COMPRESSION_GZIP            = 'gzip';
    const COMPRESSION                 = [
        self::COMPRESSION_NONE, self::COMPRESSION_BZIP, self::COMPRESSION_GZIP
    ];
    const INCLUDE_NONE                = 'none';
    const INCLUDE_ALL                 = 'all';
    const INCLUDE_SKIP_SEARCH         = 'skipSearch';
    const INCLUDE_SKIP_HISTORY        = 'skipHistory';
    const INCLUDE_SKIP_SEARCH_HISTORY = 'skipSearchHistory';
    const INCLUDE                     = [
        self::INCLUDE_ALL, self::INCLUDE_NONE, self::INCLUDE_SKIP_HISTORY,
        self::INCLUDE_SKIP_SEARCH, self::INCLUDE_SKIP_SEARCH_HISTORY
    ];
    const LOCK_TRY                    = 'try';
    const LOCK_WAIT                   = 'wait';
    const LOCK_SKIP                   = 'skip';
    const LOCK                        = [self::LOCK_SKIP, self::LOCK_TRY, self::LOCK_WAIT];

    /**
     * 
     * @param array<string> $argv
     * @return self|false
     */
    static public function fromArgv(array $argv): self | false {
        $params = [
            'targetFile'       => '',
            'configFile'       => '',
            'compression'      => self::COMPRESSION_NONE,
            'compressionLevel' => 3,
            'lock'             => self::LOCK_WAIT,
            'ifOtherBackup'    => false,
            'chunkSize'        => 0,
            'include'          => self::INCLUDE_ALL,
            'checkOnly'        => false,
            'tmpDir'           => null,
            'dateFile'         => '',
            'dateFrom'         => '',
            'dateTo'           => '',
            'dbConn'           => '',
        ];
        $help   = false;
        $nMax   = 2;
        $n      = 0;
        for ($i = 1; $i < count($argv); $i++) {
            $k = substr($argv[$i], 0, 2) === '--' ? substr($argv[$i], 2) : $n;
            if ($k === 'help') {
                $help = true;
            } elseif (is_int($k)) {
                $params[$n] = $argv[$i];
                $n++;
            } elseif (is_bool($params[$k])) {
                $params[$k] = true;
                if ($k === 'checkOnly') {
                    $nMax = 1;
                }
            } else {
                $params[$k] = $argv[$i + 1];
                $i++;
            }
        }

        $params['dateFile'] = self::sanitizePath($params['dateFile']);

        $params['targetFile'] = self::sanitizePath($params[0] ?? '', PATHINFO_FILENAME);
        $params['configFile'] = self::sanitizePath($params[1] ?? '');
        unset($params[0]);
        unset($params[1]);

        if (
            $help ||
            $n > $nMax ||
            empty($params['targetFile']) ||
            empty($params['configFile']) && !$params['checkOnly'] ||
            !in_array($params['compression'], self::COMPRESSION) ||
            !in_array($params['include'], self::INCLUDE) ||
            !in_array($params['lock'], self::LOCK)
        ) {
            return false;
        }
        return new self(...array_values($params));
    }

    static private function sanitizePath(string $path,
                                         int $pathinfo = PATHINFO_BASENAME): string {
        if (empty($path)) {
            return '';
        }
        $dir  = (string) realpath(dirname($path));
        $dir  = $dir === '/' ? '' : $dir;
        $file = pathinfo($path, $pathinfo);
        return "$dir/$file";
    }

    public function __construct(
        public readonly string $targetFile, public readonly string $configFile,
        public readonly string $compression,
        public readonly int $compressionLevel, public readonly string $lock,
        public readonly bool $ifOtherBackup, public readonly int $chunkSize,
        public readonly string $include, public readonly bool $checkOnly,
        public readonly string | null $tmpDir, public readonly string $dateFile,
        public readonly string $dateFrom, public readonly string $dateTo,
        public readonly string $dbConn,
    ) {
        
    }
}
