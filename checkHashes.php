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

$params = [];
$n      = 0;
$argv ??= [];
for ($i = 1; $i < count($argv); $i++) {
    if (substr($argv[$i], 0, 2) === '--') {
        $params[substr($argv[$i], 2)] = $argv[$i + 1];
        $i++;
    } else {
        $params[$n] = $argv[$i];
        $n++;
    }
}

if (($argc ?? 0) < 2) {
    exit(<<<AAA
checkHashes.php repoConfigFile

Checks repository storage consistency by comparing hashes stored in the metadata
with actual file hashes.

Parameters:
    repoConfigFile path to the repository config yaml file


AAA
    );
}

$t = microtime(true);
printf("-----\nRunning %s on %s\n", implode(" ", $argv), date('Y-m-d H:i:s', (int) $t));

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
    throw new Exception('Storage dir set up as a relative path in the repository config file - can not determine paths');
}

$pdo   = new PDO($cfg->dbConnStr->guest);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$query = $pdo->prepare("SELECT id, value AS hash FROM metadata WHERE property = ? ORDER BY id");
$query->execute([$cfg->schema->hash]);

function getStorageDir(int $id, string $path, int $level, int $levelMax): string {
    if ($level < $levelMax) {
        $path = sprintf('%s/%02d', $path, $id % 100);
        $path = getStorageDir((int) $id / 100, $path, $level + 1, $levelMax);
    }
    return $path;
}

$count = ['n' => 0, 'missing' => 0, 'mismatch' => 0];
while ($res   = $query->fetchObject()) {
    $count['n']++;
    $path = getStorageDir($res->id, $cfg->storage->dir, 0, $cfg->storage->levels) . '/' . $res->id;
    if (!file_exists($path)) {
        printf("error: resource %d doesn't exist [%d]\n", $res->id, $count['n']);
        $count['missing']++;
    } else {
        $p     = strpos($res->hash, ':');
        $algo  = substr($res->hash, 0, $p);
        $hash1 = substr($res->hash, $p + 1);
        $hash2 = hash_file($algo, $path);
        if ($hash1 === $hash2) {
            printf("ok: resource %d [%d]\n", $res->id, $count['n']);
        } else {
            printf("error: resource %d hash doesn't match %s != %s (%s) [%d]\n", $res->id, $hash1, $hash2, $algo, $count['n']);
            $count['mismatch']++;
        }
    }
}

$t = microtime(true) - $t;
printf("%d files checked in %.1f s\t%d missing\t%d mismatches\n", $count['n'], $t, $count['missing'], $count['mismatch']);
exit($count['missing'] + $count['mismatch'] != 0);
