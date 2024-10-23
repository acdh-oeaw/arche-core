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

use Composer\Autoload\ClassLoader;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use rdfInterface\DatasetNodeInterface;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\NQuadsParser;
use quickRdfIo\NQuadsSerializer;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\Config;
use function \GuzzleHttp\json_encode;

/**
 * Description of CallbackController
 *
 * @author zozlak
 */
class HandlersController {

    const TYPE_RPC  = 'rpc';
    const TYPE_FUNC = 'function';

    /**
     *
     * @var \PhpAmqpLib\Connection\AMQPStreamConnection
     */
    private $rmqConn;

    /**
     *
     * @var \PhpAmqpLib\Channel\AMQPChannel
     */
    private $rmqChannel;

    /**
     *
     * @var string
     */
    private $rmqQueue;

    /**
     *
     * @var array<string, array<object>>
     */
    private $handlers = [];

    /**
     *
     * @var array<string, object|null>
     */
    private $queue;

    /**
     *
     * @var float
     */
    private $rmqTimeout = 1;

    /**
     *
     * @var bool
     */
    private $rmqExceptionOnTimeout = true;

    public function __construct(Config $cfg, ClassLoader $loader) {
        if (isset($cfg->rabbitMq)) {
            RC::$log->info('Initializing rabbitMQ connection');

            $this->rmqTimeout            = (float) $cfg->rabbitMq->timeout;
            $this->rmqExceptionOnTimeout = (bool) $cfg->rabbitMq->exceptionOnTimeout;

            $this->rmqConn    = new AMQPStreamConnection($cfg->rabbitMq->host, (int) $cfg->rabbitMq->port, $cfg->rabbitMq->user, $cfg->rabbitMq->password);
            $this->rmqChannel = $this->rmqConn->channel();
            list($this->rmqQueue,, ) = $this->rmqChannel->queue_declare('', false, false, true, false);
            $clbck            = [$this, 'callback'];
            $this->rmqChannel->basic_consume($this->rmqQueue, '', false, true, false, false, $clbck);
        }
        $this->handlers = array_map(fn(array | null $x) => $x ?? [], (array) $cfg->methods);

        foreach ((array) ($cfg->classLoader ?? []) as $nmsp => $path) {
            $nmsp = preg_replace('|\$|', '', $nmsp) . "\\";
            $loader->addPsr4($nmsp, $path);
        }

        $info = [];
        foreach ($this->handlers as $k => $v) {
            $info[] = "$k(" . count($v) . ")";
        }
        RC::$log->info('Registered handlers: ' . implode(', ', $info));
    }

    public function __destruct() {
        if ($this->rmqChannel !== null) {
            $this->rmqChannel->close();
        }
        if ($this->rmqConn !== null) {
            $this->rmqConn->close();
        }
    }

    public function hasHandlers(string $method): bool {
        return isset($this->handlers[$method]);
    }

    public function handleResource(string $method, int $id,
                                   DatasetNodeInterface $res, ?string $path): DatasetNodeInterface {
        if (!isset($this->handlers[$method])) {
            return $res;
        }
        foreach ($this->handlers[$method] as $i) {
            switch ($i->type) {
                case self::TYPE_RPC:
                    $res = $this->callRpcResource($method, $i->queue, $id, $res, $path);
                    break;
                case self::TYPE_FUNC:
                    $res = $this->callFunction($i->function, $id, $res, $path);
                    break;
                default:
                    throw new RepoException('Unknown handler type: ' . $i->type, 500);
            }
        }
        return $res;
    }

    /**
     * 
     * @param string $method
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     * @throws RepoException
     */
    public function handleTransaction(string $method, int $txId,
                                      array $resourceIds): void {
        $methodKey = 'tx' . strtoupper(substr($method, 0, 1)) . substr($method, 1);
        if (!isset($this->handlers[$methodKey])) {
            return;
        }
        foreach ($this->handlers[$methodKey] as $i) {
            switch ($i->type) {
                case self::TYPE_RPC:
                    $data = json_encode([
                        'method'        => $method,
                        'transactionId' => $txId,
                        'resourceIds'   => $resourceIds,
                    ]);
                    $res  = $this->sendRmqMessage($i->queue, $data);
                    break;
                case self::TYPE_FUNC:
                    $res  = $this->callFunction($i->function, $method, $txId, $resourceIds);
                    break;
                default:
                    throw new RepoException('Unknown handler type: ' . $i->type, 500);
            }
        }
    }

    private function callRpcResource(string $method, string $queue, int $id,
                                     DatasetNode $res, ?string $path): DatasetNode {
        $serializer = new NQuadsSerializer();
        $data       = json_encode([
            'method'   => $method,
            'path'     => $path,
            'uri'      => $res->getNode()->getValue(),
            'id'       => $id,
            'metadata' => $serializer->serialize($res),
        ]);
        $result     = $this->sendRmqMessage($queue, $data);
        if ($result === null) {
            $result = $res;
        } else {
            $result = (new DatasetNode($res->getNode()))->withDataset($result);
        }
        return $result;
    }

    /**
     * 
     * @param string $queue
     * @param string $data
     * @return null|Dataset
     * @throws RepoException
     */
    private function sendRmqMessage(string $queue, string $data): null | Dataset {
        $id               = uniqid();
        RC::$log->debug("\tcalling RPC handler with id $id using the $queue queue");
        $opts             = ['correlation_id' => $id, 'reply_to' => $this->rmqQueue];
        $msg              = new AMQPMessage($data, $opts);
        $this->rmqChannel->basic_publish($msg, '', $queue);
        $this->queue[$id] = null;
        try {
            $this->rmqChannel->wait(null, false, $this->rmqTimeout);
        } catch (AMQPTimeoutException $e) {
            
        }
        if ($this->queue[$id] === null) {
            if ($this->rmqExceptionOnTimeout) {
                throw new RepoException("$queue handler timeout", 500);
            }
            RC::$log->debug("\tRPC handler with id $id using the $queue queue ended with a timeout");
            return null;
        }
        RC::$log->debug("\tRPC handler with id $id using the $queue queue ended");
        return $this->queue[$id];
    }

    /**
     * 
     * @param string $func
     * @param mixed $params
     * @return mixed
     */
    private function callFunction(string $func, ...$params) {
        RC::$log->debug("\tcalling function handler $func()");
        if (is_callable($func)) {
            $result = $func(...$params);
            RC::$log->debug("\tfunction handler $func() ended");
        } else {
            throw new RepoException("Handler $func does not exist", 500);
        }
        return $result;
    }

    /**
     * Handles the AMQP message received from a handler.
     * 
     * The message should be a JSON with at least `status` property.
     * Status 0 means successful execution and any other value indicates an error.
     * 
     * In case of error an exception is being thrown with the exception code
     * equal to the `status` property value and exception message read from 
     * the `message` field of the response (if it's not preset, a generic
     * error message is used).
     * 
     * In `status` equals 0, then the `metadata` property value (or an empty
     * string if this property doesn't exist) is parsed as application/n-triples.
     * 
     * @param AMQPMessage $msg
     * @return void
     */
    public function callback(AMQPMessage $msg): void {
        $id = $msg->get('correlation_id');
        RC::$log->debug("\t\tresponse with id $id received");
        if (key_exists($id, $this->queue)) {
            $msg = json_decode($msg->body);
            if (!is_object($msg) || !isset($msg->status)) {
                throw new RepoException('Wrong response from a handler', 500);
            }
            if ($msg->status !== 0) {
                throw new RepoException($msg->message ?? 'Non-zero handler status', $msg->status);
            }
            $parser           = new NQuadsParser(new DF(), false, NQuadsParser::MODE_TRIPLES);
            $graph            = new Dataset();
            $graph->add($parser->parse($msg->metadata ?? ''));
            $this->queue[$id] = $graph;
        }
    }
}
