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

namespace acdhOeaw\arche\core\tests;

use PDO;
use RuntimeException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Channel\AMQPChannel;
use simpleRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use quickRdfIo\NQuadsParser;
use quickRdfIo\NQuadsSerializer;
use termTemplates\QuadTemplate as QT;
use zozlak\logging\Log;
use acdhOeaw\arche\core\RepoException;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\Config;

/**
 * 
 * @param string $method
 * @param int $txId
 * @param array<int> $resourceIds
 * @return void
 */
function txCommit(string $method, int $txId, array $resourceIds): void {
    Handler::onTxCommit($method, $txId, $resourceIds);
}

/**
 * Bunch of test handlers
 *
 * @author zozlak
 */
class Handler {

    const CHECKTRIGGER_PROP = 'http://bar';
    const CHECK_PROP        = 'http://foo';

    static public function brokenHandler(): void {
        throw new \Exception('', 123);
    }

    /**
     * 
     * @param array<int> $resourceIds
     * @return void
     */
    static public function onTxCommit(string $method, int $txId,
                                      array $resourceIds): void {
        RC::$log->debug("\t\ton$method handler for " . $txId);

        $cfg   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $pdo   = new PDO($cfg['dbConn']['admin']);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->beginTransaction();
        $query = $pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value) 
            VALUES (?, 'https://commit/property', 'http://www.w3.org/2001/XMLSchema#string', '', ?)
        ");
        foreach ($resourceIds as $i) {
            $query->execute([$i, $method . $txId]);
        }
        $pdo->commit();
    }

    static public function deleteReference(int $id, DatasetNode $meta,
                                           ?string $path): DatasetNode {
        if ($meta->any(new QT(predicate: self::CHECKTRIGGER_PROP)) && $meta->none(new QT(predicate: self::CHECK_PROP))) {
            throw new RepoException(self::CHECK_PROP . " is missing");
        }
        return $meta;
    }

    static public function throwException(int $id, DatasetNode $meta,
                                          ?string $path): DatasetNode {
        throw new RepoException("Just throw an exception", 400);
    }

    /**
     * 
     * @param array<int> $resourceIds
     * @return void
     */
    static public function longError(string $method, int $txId,
                                      array $resourceIds): void {
        throw new RepoException(str_repeat('0123456789', 10000), 400);
    }
    
    private AMQPStreamConnection $rmqConn;
    private AMQPChannel $rmqChannel;
    private Log $log;

    public function __construct(string $configFile) {
        $cfg       = Config::fromYaml($configFile);
        $this->log = new Log($cfg->rest->logging->file, $cfg->rest->logging->level);
        $cfg       = $cfg->rest->handlers;
        $rCfg      = $cfg->rabbitMq ?: throw new RuntimeException("Bad config");

        $this->rmqConn    = new AMQPStreamConnection($rCfg->host, (int) $rCfg->port, $rCfg->user, $rCfg->password);
        $this->rmqChannel = $this->rmqConn->channel();
        $this->rmqChannel->basic_qos(0, 1, false);

        foreach ($cfg->methods as $method) {
            foreach ($method as $h) {
                /** @var object $h */
                if ($h->type === 'rpc') {
                    $this->rmqChannel->queue_declare($h->queue, false, false, false, false);
                    $clbck = [$this, $h->queue];
                    if (is_callable($clbck)) {
                        $this->rmqChannel->basic_consume($h->queue, '', false, false, false, false, $clbck);
                    } else {
                        throw new RuntimeException("Can't create a handler");
                    }
                }
            }
        }
    }

    public function __destruct() {
        $this->rmqChannel->close();
        $this->rmqConn->close();
    }

    public function loop(): void {
        while ($this->rmqChannel->is_consuming()) {
            $this->rmqChannel->wait();
        }
    }

    public function onUpdateRpc(AMQPMessage $req): void {
        $this->log->debug("\t\tonUpdateRpc");
        $data = $this->parse($req->body);
        $this->log->debug("\t\t\tfor " . $data->uri);

        usleep(300000);
        $data->meta->add(DF::quadNoSubject(DF::namedNode('https://rpc/property'), DF::literal('update rpc')));

        $rdf  = $this->serialize($data->meta);
        $body = json_encode(['status' => 0, 'metadata' => $rdf]);
        $opts = ['correlation_id' => $req->get('correlation_id')];
        $msg  = new AMQPMessage($body, $opts);
        $req->delivery_info['channel']->basic_publish($msg, '', $req->get('reply_to'));
        $req->delivery_info['channel']->basic_ack($req->delivery_info['delivery_tag']);
    }

    public function onCreateRpc(AMQPMessage $req): void {
        $this->log->debug("\t\tonCreateRpc");
        $data = $this->parse($req->body);
        $this->log->debug("\t\t\tfor " . $data->uri);

        $data->meta->add(DF::quadNoSubject(DF::namedNode('https://rpc/property'), DF::literal('create rpc')));

        $rdf  = $this->serialize($data->meta);
        $body = json_encode(['status' => 0, 'metadata' => $rdf]);
        $opts = ['correlation_id' => $req->get('correlation_id')];
        $msg  = new AMQPMessage($body, $opts);
        $req->delivery_info['channel']->basic_publish($msg, '', $req->get('reply_to'));
        $req->delivery_info['channel']->basic_ack($req->delivery_info['delivery_tag']);
    }

    public function onCreateRpcError(AMQPMessage $req): void {
        $this->log->debug("\t\tonCreateRpcError");
        $data = $this->parse($req->body);
        $this->log->debug("\t\t\tfor " . $data->uri);

        $body = json_encode(['status' => 400, 'message' => 'metadata is always wrong']);
        $opts = ['correlation_id' => $req->get('correlation_id')];
        $msg  = new AMQPMessage($body, $opts);
        $req->delivery_info['channel']->basic_publish($msg, '', $req->get('reply_to'));
        $req->delivery_info['channel']->basic_ack($req->delivery_info['delivery_tag']);
    }

    public function onCommitRpc(AMQPMessage $req): void {
        $this->log->debug("\t\tonCommitRpc");
        $data = json_decode($req->body); // method, transactionId, resourceIds
        $this->log->debug("\t\t\tfor " . $data->method . " on transaction " . $data->transactionId);

        $cfg   = yaml_parse_file(__DIR__ . '/../config.yaml');
        $pdo   = new PDO($cfg['dbConn']['admin']);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->beginTransaction();
        $query = $pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value) 
            VALUES (?, 'https://commit/property', 'http://www.w3.org/2001/XMLSchema#string', '', ?)
        ");
        foreach ($data->resourceIds as $i) {
            $query->execute([$i, $data->method . $data->transactionId]);
        }
        $pdo->commit();

        $opts = ['correlation_id' => $req->get('correlation_id')];
        $body = json_encode(['status' => 0]);
        $msg  = new AMQPMessage($body, $opts);
        $req->delivery_info['channel']->basic_publish($msg, '', $req->get('reply_to'));
        $req->delivery_info['channel']->basic_ack($req->delivery_info['delivery_tag']);
    }

    private function parse(string $msg): object {
        $data       = json_decode($msg);
        $parser     = new NQuadsParser(new DF(), false, NQuadsParser::MODE_TRIPLES);
        $data->meta = new DatasetNode(DF::namedNode($data->uri));
        $data->meta->add($parser->parse($data->metadata));
        return $data;
    }

    private function serialize(DatasetNode $dataset): string {
        $serializer = new NQuadsSerializer();
        return $serializer->serialize($dataset->getDataset());
    }
}
