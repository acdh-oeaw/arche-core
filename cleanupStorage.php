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

$params = ['ageToDelete' => 604800];
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

if (!isset($params[0])) {
    exit(<<<AAA
cleanupStorage.php [--ageToDelete seconds] repoConfigFile

Searches for files in the repository's storage directory which do not 
correspond to repository resources and optionally removes them.

Parameters:
    repoConfigFile path to the repository config yaml file
    --ageTodelete  minimum age in seconds of a file which doesn't have 
                   a corresponding repository resource causing the file to be 
                   removed (by default 604800 which equals one week)


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

/**
 * @return Generator<string>
 */
function iterateDir(string $path, int $level, int $levels): Generator {
    $dir = @opendir($path);
    if ($dir === false) {
        return;
    }
    while ($file = readdir($dir)) {
        if ($file == '.' || $file == '..') {
            continue;
        }
        $fullPath = $path . '/' . $file;
        if (is_dir($fullPath)) {
            yield from iterateDir($fullPath, $level + 1, $levels);
        } elseif ($level === $levels) {
            yield $fullPath;
        }
    }
    closedir($dir);
}

$t = microtime(true);

$count       = $missing     = $deleted     = $sizeMissing = $sizeDeleted = 0;
$pdo         = new PDO($cfg->dbConnStr->guest);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$query       = $pdo->prepare("SELECT count(*) FROM resources WHERE id = ?");
foreach (iterateDir($cfg->storage->dir, 0, $cfg->storage->levels) as $file) {
    $count++;
    $id     = basename($file);
    $exists = false;
    if ((string) ((int) $id) === $id) {
        $query->execute([$id]);
        $exists = $query->fetchColumn() === 1;
    }
    if (!$exists) {
        $size        = filesize($file) / 1024 / 1024;
        $missing++;
        $sizeMissing += $size;
        $age = time() - filemtime($file);
        if ($age > $params['ageToDelete']) {
            printf("deleting %s (%d MB, %d s old)\n", $file, $size, $age);
            unlink($file);
            $deleted++;
            $sizeDeleted += $size;
        } else {
            printf("missing %s (%d MB, %d s old)\n", $file, $size, $age);
        }
    }
}

printf("Out of %d files checked %d files were missing (%d MB) and %d files were deleted (%d MB)\n", $count, $missing, $sizeMissing, $deleted, $sizeDeleted);

printf("Processing time %.1f s\n", microtime(true) - $t);
exit($missing > 0);
