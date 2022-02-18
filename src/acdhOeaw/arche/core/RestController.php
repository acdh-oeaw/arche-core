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

use ErrorException;
use PDO;
use PDOException;
use Throwable;
use Composer\Autoload\ClassLoader;
use zozlak\logging\Log as Log;
use acdhOeaw\arche\core\Transaction;
use acdhOeaw\arche\lib\Config;
use acdhOeaw\arche\lib\exception\RepoLibException;
use acdhOeaw\arche\core\util\OutputFile;

/**
 * Description of RestController
 *
 * @author zozlak
 */
class RestController {

    const ID_CREATE    = 0;
    const ACCESS_READ  = 1;
    const ACCESS_WRITE = 2;
    const CORS_ORIGIN  = '__origin__';

    /**
     * 
     * @var array<string, string>
     */
    static private $requestParam = [
        'metadataReadMode'       => 'readMode',
        'metadataParentProperty' => 'parentProperty',
        'metadataWriteMode'      => 'writeMode',
        'transactionId'          => 'transactionId',
        'withReferences'         => 'withReferences',
    ];

    /**
     * 
     * @var array<string, string>
     */
    static private $outputFormats = [
        'text/turtle'           => 'text/turtle',
        'application/rdf+xml'   => 'application/rdf+xml',
        'application/n-triples' => 'application/n-triples',
        'application/ld+json'   => 'application/ld+json',
        '*/*'                   => 'text/turtle',
        'text/*'                => 'text/turtle',
        'application/*'         => 'application/n-triples',
    ];
    static public Config $config;
    static public Log $log;
    static public PDO $pdo;
    static public Transaction $transaction;
    static public Resource $resource;
    static public Auth $auth;
    static public int $logId;
    static private string | OutputFile | MetadataReadOnly $output;

    /**
     * 
     * @var array<string, array<string>>
     */
    static private array $headers;

    /**
     *
     * @var \acdhOeaw\arche\core\HandlersController
     */
    static public $handlersCtl;

    static public function init(string $configFile, ClassLoader $loader): void {
        set_error_handler(function ($errno, $errstr, $errfile, $errline) {
            if (0 === error_reporting()) {
                return false;
            }
            throw new ErrorException($errstr, 500, $errno, $errfile, $errline);
        });

        self::$config  = Config::fromYaml($configFile);
        self::$output  = '';
        self::$headers = [];

        self::$logId = rand(0, 999999); // short unique request id
        self::$log   = new Log(
            self::$config->rest->logging->file,
            self::$config->rest->logging->level,
            "{TIMESTAMP}\t" . sprintf("%06d", self::$logId) . "\t{LEVEL}\t{MESSAGE}"
        );

        try {
            self::$log->info("------------------------------");
            self::$log->info(filter_input(INPUT_SERVER, 'REQUEST_METHOD') . " " . filter_input(INPUT_SERVER, 'REQUEST_URI'));

            self::$pdo   = new PDO(self::$config->dbConn->admin);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            self::$pdo->query("SET application_name TO rest_" . self::$logId);
            $lockTimeout = (int) (self::$config->transactionController->lockTimeout ?? Transaction::LOCK_TIMEOUT_DEFAULT);
            self::$pdo->query("SET lock_timeout TO $lockTimeout");
            $stmtTimeout = (int) (self::$config->transactionController->statementTimeout ?? Transaction::STMT_TIMEOUT_DEFAULT);
            self::$pdo->query("SET statement_timeout TO $stmtTimeout");

            self::$transaction = new Transaction();

            self::$auth = new Auth();

            self::$handlersCtl = new HandlersController(new Config(self::$config->rest->handlers), $loader);
        } catch (BadRequestException $e) {
            self::$log->error($e);
            http_response_code($e->getCode());
            echo $e->getMessage();
        } catch (Throwable $e) {
            self::$log->error($e);
            http_response_code(500);
        }
    }

    static public function handleRequest(): void {
        if (http_response_code() !== 200) {
            // if the response code has been set already, don't do anything else
            return;
        }
        try {
            if (!empty(self::$config->rest->cors ?? '')) {
                $origin = self::$config->rest->cors;
                if ($origin === self::CORS_ORIGIN) {
                    $origin = filter_input(INPUT_SERVER, 'HTTP_ORIGIN') ?? '*';
                    header('Vary: origin');
                }
                header("Access-Control-Allow-Origin: $origin");
                header('Access-Control-Allow-Headers: Accept, Content-Type');
                header('Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS');
                header('Cache-Control: no-cache');
            }

            self::$pdo->beginTransaction();

            $method   = filter_input(INPUT_SERVER, 'REQUEST_METHOD');
            $method   = ucfirst(strtolower($method));
            $path     = substr(filter_input(INPUT_SERVER, 'REQUEST_URI'), strlen(self::$config->rest->pathBase));
            $queryPos = strpos($path, '?');
            if ($queryPos !== false) {
                $path = substr($path, 0, $queryPos);
            }

            if ($path === 'describe') {
                $describe = new Describe();
                if (method_exists($describe, $method)) {
                    $describe->$method();
                } else {
                    $describe->options(405);
                }
            } elseif ($path === 'transaction') {
                self::$log->info("Transaction->$method()");
                if (method_exists(self::$transaction, $method)) {
                    self::$transaction->$method();
                } else {
                    self::$transaction->options(405);
                }
            } elseif (preg_match('@^user/?$|^user/[^/]+/?$@', $path)) {
                $userApi = new UserApi();
                $user    = substr($path, 5);
                if (method_exists($userApi, $method) && (!empty($user) || $method === 'Get')) {
                    $userApi->$method($user);
                } else {
                    $userApi->options('', 405);
                }
            } elseif ($path === 'search') {
                $search = new Search();
                if (method_exists($search, $method)) {
                    $search->$method();
                } else {
                    $search->options(405);
                }
            } elseif (preg_match('>^([0-9]+/?)?(metadata|tombstone)?$>', $path)) {
                $collection = $suffix     = '';
                $id         = null;
                if (is_numeric(substr($path, 0, 1))) {
                    $id = (int) $path; // PHP is very permissive when casting to numbers
                } else {
                    $collection = 'Collection';
                }
                $matches = null;
                if (preg_match('>metadata|tombstone$>', $path, $matches)) {
                    $suffix = ucfirst($matches[0]);
                }

                self::$resource = new Resource($id);
                $methodFull     = $method . $collection . $suffix;
                self::$log->info("Resource($id)->$methodFull()");
                if (method_exists(self::$resource, $methodFull)) {
                    self::$resource->$methodFull();
                } else {
                    $methodOptions = 'options' . $collection . $suffix;
                    self::$resource->$methodOptions(405);
                }
            } else if ($method === 'Put' && preg_match('>^merge/([0-9]+)/([0-9]+)/?$>', $path, $matches)) {
                self::$resource = new Resource($matches[2]);
                self::$resource->merge($matches[1]);
            } else {
                throw new RepoException('Not Found', 404);
            }

            // free the database connection
            if (self::$output instanceof MetadataReadOnly) {
                self::setHeader('Content-Type', self::$headers['content-type'][0] ?? self::$config->rest->defaultMetadataFormat);
                self::$output->generateOutput(self::$headers['content-type'][0]);
            }
            self::$transaction->prolong();
            self::$pdo->commit();
        } catch (BadRequestException $ex) {
            $statusCode = $ex->getCode();
            echo $ex->getMessage();
        } catch (RepoLibException $ex) {
            $statusCode = $ex->getCode() >= 100 ? $ex->getCode() : 500;
            echo $ex->getMessage();
        } catch (PDOException $ex) {
            $statusCode = $ex->getCode() === Transaction::PG_LOCK_FAILURE ? 409 : 500;
            echo $ex->getMessage();
        } catch (Throwable $ex) {
            self::$log->error($ex);
            $statusCode = 500;
        } finally {
            if (isset($ex)) {
                if (self::$pdo->inTransaction()) {
                    self::$pdo->rollBack();
                }

                self::$log->error($ex);
                if (self::$config->transactionController->enforceCompleteness && self::$transaction->getId() !== null) {
                    self::$log->info('aborting transaction ' . self::$transaction->getId() . ' due to enforce completeness');
                    self::$transaction->unlockResources(self::$logId);
                    while (self::$transaction->getState() === Transaction::STATE_ACTIVE) {
                        try {
                            self::$transaction->delete();
                        } catch (ConflictException) {
                            
                        }
                    }
                }
                self::$output  = '';
                self::$headers = [];
            }
            if (isset($statusCode)) {
                http_response_code($statusCode);
            }
            // output the response AFTER setting the HTTP response code
            self::sendOutput();

            self::$transaction->unlockResources(self::$logId);

            self::$log->info("Return code " . http_response_code());
            self::$log->debug("Memory usage " . round(memory_get_peak_usage(true) / 1024 / 1024) . " MB");
        }
    }

    static public function getBaseUrl(): string {
        return self::$config->rest->urlBase . self::$config->rest->pathBase;
    }

    static public function getHttpHeaderName(string $purpose): string {
        return 'HTTP_' . str_replace('-', '_', self::$config->rest->headers->$purpose ?? throw new RepoException("Unknown HTTP header name for $purpose"));
    }

    static public function getRequestParameter(string $purpose): ?string {
        return filter_input(\INPUT_GET, self::$requestParam[$purpose]) ??
            filter_input(\INPUT_SERVER, self::getHttpHeaderName($purpose));
    }

    /**
     * 
     * @param string | OutputFile | MetadataReadOnly $output
     * @param string|null $mimeType
     * @return void
     */
    static public function setOutput(string | OutputFile | MetadataReadOnly $output,
                                     ?string $mimeType = null): void {
        self::$output = $output;
        if (!empty($mimeType)) {
            self::setHeader('Content-Type', $mimeType);
        }
    }

    static public function appendOutput(string $output): void {
        if (!is_string(self::$output)) {
            throw new RepoException("Can't mix string and non-string output");
        }
        self::$output .= $output;
    }

    static public function setHeader(string $header, string $value): void {
        $header                 = strtolower($header);
        self::$headers[$header] = [$value];
    }

    static public function addHeader(string $header, string $value): void {
        $header = strtolower($header);
        if (!isset(self::$headers[$header])) {
            self::$headers[$header] = [];
        }
        self::$headers[$header][] = $value;
    }

    static private function sendOutput(): void {
        foreach (self::$headers as $header => $values) {
            foreach ($values as $v) {
                header("$header: $v", false);
            }
        }
        if (is_object(self::$output)) {
            self::$output->sendOutput();
        } else {
            echo self::$output;
        }
        self::$output  = '';
        self::$headers = [];
    }
}
