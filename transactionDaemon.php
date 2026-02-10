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

/**
 * Spawns a transaction controller daemon - a service which keeps a pre-transaction
 * database state for every transaction allowing to rollback them.
 */
$argv ??= [];
require_once ($argv[2] ?? __DIR__) . '/vendor/autoload.php';

if (($argc ?? 0) < 2) {
    exit("Usage:\n  " . $argv[0] . " configFile\n");
}

if (file_exists(__DIR__ . '/build/logs')) {
    xdebug_start_code_coverage();
}

$controller = new acdhOeaw\arche\core\TransactionController($argv[1]);

pcntl_async_signals(true);
pcntl_signal(SIGTERM, function () {
    global $controller;
    $controller->stop();
});
pcntl_signal(SIGINT, function () {
    global $controller;
    $controller->stop();
});
pcntl_signal(SIGUSR1, function () {
    global $controller;
    $controller->loadConfig();
});

$controller->handleRequests();

if (file_exists(__DIR__ . '/build/logs')) {
    file_put_contents(__DIR__ . '/build/logs/' . microtime(true) . '.json', json_encode(xdebug_get_code_coverage()));
}
