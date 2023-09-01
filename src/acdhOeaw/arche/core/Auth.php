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

use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\AuthInterface;
use zozlak\queryPart\QueryPart;
use zozlak\auth\AuthController;
use zozlak\auth\usersDb\PdoDb;
use function \GuzzleHttp\json_encode;

/**
 * Description of Auth
 *
 * @author zozlak
 */
class Auth implements AuthInterface {

    const DEFAULT_ALLOW = 'allow';
    const DEFAULT_DENY  = 'deny';

    /**
     * 
     * @var AuthController
     */
    private $controller;

    /**
     * 
     * @var string
     */
    private $userName;

    /**
     * 
     * @var array<string>
     */
    private $userRoles;

    /**
     * 
     * @var bool
     */
    private $isAdmin;

    /**
     * 
     * @var bool
     */
    private $isCreator;

    public function __construct() {
        $cfg              = RC::$config->accessControl;
        $db               = new PdoDb($cfg->db->connStr, $cfg->db->table, $cfg->db->userCol, $cfg->db->dataCol);
        $this->controller = new AuthController($db);

        foreach (RC::$config->accessControl->authMethods as $i) {
            $class  = $i->class;
            $method = new $class(...$i->parameters);
            $this->controller->addMethod($method);
        }

        $this->controller->authenticate();

        $this->userName = $this->controller->getUserName();

        $this->userRoles = array_merge(
            [$this->userName],
            $this->controller->getUserData()->groups ?? []
        );
        if (isset($cfg->publicRole)) {
            $this->userRoles[] = $cfg->publicRole;
        }

        $this->isAdmin   = count(array_intersect($this->userRoles, $cfg->adminRoles)) > 0;
        $this->isCreator = count(array_intersect($this->userRoles, $cfg->create->allowedRoles)) > 0;
    }

    public function checkCreateRights(): void {
        $c = RC::$config->accessControl;
        if (!$this->isAdmin && !$this->isCreator) {
            RC::$log->debug(json_encode(['roles' => $this->userRoles, 'allowed' => $c->create->allowedRoles]));
            throw new RepoException('Resource creation denied', 403);
        }
    }

    public function checkAccessRights(int $resId, string $privilege,
                                      bool $metadataRead): void {
        $c = RC::$config->accessControl;
        if ($metadataRead && !$c->enforceOnMetadata || $this->isAdmin) {
            return;
        }
        $query   = RC::$pdo->prepare("SELECT json_agg(value) AS val FROM metadata WHERE id = ? AND property = ?");
        $query->execute([$resId, $c->schema->$privilege]);
        $allowed = (string) $query->fetchColumn();
        $allowed = json_decode($allowed) ?? [];
        $default = $c->defaultAction->$privilege ?? self::DEFAULT_DENY;
        if (count(array_intersect($this->userRoles, $allowed)) === 0 && $default !== self::DEFAULT_ALLOW) {
            RC::$log->debug(json_encode(['roles' => $this->userRoles, 'allowed' => $allowed]));
            throw new RepoException('Forbidden', 403);
        }
    }

    public function batchCheckAccessRights(string $table, string $privilege,
                                           bool $metadataRead): void {
        $c       = RC::$config->accessControl;
        $default = $c->defaultAction->$privilege ?? self::DEFAULT_DENY;
        if ($metadataRead && !$c->enforceOnMetadata || $this->isAdmin || $default === self::DEFAULT_ALLOW) {
            return;
        }
        $query     = "
            SELECT string_agg(id::text, ', ') AS forbidden
            FROM $table r
            WHERE NOT EXISTS (
                SELECT 1
                FROM metadata
                WHERE
                    id = r.id
                    AND property = ?
                    AND substring(value, 1, 1000) IN (" . substr(str_repeat(', ?', count($this->userRoles)), 2) . ")
            )
        ";
        $query     = RC::$pdo->prepare($query);
        $query->execute(array_merge([$c->schema->$privilege], $this->userRoles));
        $forbidden = $query->fetchColumn();
        if (!empty($forbidden)) {
            RC::$log->debug("Forbidden resources: $forbidden");
            throw new RepoException('Forbidden', 403);
        }
    }

    public function getCreateRights(): DatasetNode {
        $c     = RC::$config->accessControl;
        $node  = DF::blankNode();
        $graph = new DatasetNode($node);

        $role = $this->getUserName();
        foreach ($c->create->creatorRights as $i) {
            $graph->add(DF::quad($node, DF::namedNode($c->schema->$i), DF::literal($role)));
        }

        foreach ($c->create->assignRoles as $privilege => $roles) {
            foreach ($roles as $role) {
                $prop = $c->schema->$privilege;
                $graph->add(DF::quad($node, DF::namedNode($prop), DF::literal($role)));
            }
        }

        return $graph;
    }

    /**
     * Returns (if needed according to the config) an SQL query returning a list
     * of resource ids the current user can read.
     * @return QueryPart
     */
    public function getMetadataAuthQuery(): QueryPart {
        $c = RC::$config->accessControl;
        if ($c->enforceOnMetadata && !$this->isAdmin) {
            return new QueryPart(
                " JOIN (SELECT * from get_allowed_resources(?, ?)) maq USING (id) ",
                [$c->schema->read, json_encode($this->getUserRoles())]
            );
        } else {
            return new QueryPart();
        }
    }

    public function getUserName(): string {
        return $this->userName;
    }

    /**
     * 
     * @return string[]
     */
    public function getUserRoles(): array {
        return $this->userRoles;
    }

    public function isAdmin(): bool {
        return $this->isAdmin;
    }
}
