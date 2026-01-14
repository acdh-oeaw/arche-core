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

    const DEFAULT_ALLOW  = 'allow';
    const DEFAULT_DENY   = 'deny';
    const DICT_ADVERTISE = [
        'none'   => AuthController::ADVERTISE_NONE,
        'once'   => AuthController::ADVERTISE_ONCE,
        'always' => AuthController::ADVERTISE_ALWAYS,
    ];

    private AuthController $controller;
    private string $userName;
    private bool $isAdmin;
    private bool $isCreator;
    private bool $authenticated;
    private bool $isPublic;

    /**
     * 
     * @var array<string>
     */
    private array $userRoles;

    public function __construct() {
        $cfg              = RC::$config->accessControl;
        $db               = new PdoDb(RC::$pdo, $cfg->db->table, $cfg->db->userCol, $cfg->db->dataCol);
        $this->controller = new AuthController($db);

        foreach (RC::$config->accessControl->authMethods as $i) {
            $class  = $i->class;
            $method = new $class(...$i->parameters);
            $this->controller->addMethod($method, self::DICT_ADVERTISE[$i->advertise ?? 'once']);
        }

        $this->authenticated = $this->controller->authenticate(true);

        if ($this->authenticated) {
            $this->userName  = $this->controller->getUserName();
            $this->userRoles = array_merge(
                [$this->userName, $cfg->publicRole, $cfg->loggedInRole],
                $this->controller->getUserData()->groups ?? []
            );
        } else {
            $this->userName  = $cfg->publicRole;
            $this->userRoles = [$this->userName];
        }

        $this->isAdmin   = count(array_intersect($this->userRoles, $cfg->adminRoles)) > 0;
        $this->isCreator = count(array_intersect($this->userRoles, $cfg->create->allowedRoles)) > 0;
        $this->isPublic  = $this->userName === $cfg->publicRole;

        $cookieName = $cfg->cookie->name ?? '';
        if (!empty($cookieName) && !$this->isPublic()) {
            RC::addHeader('set-cookie', $this->getCookieHeaderValue($cookieName, implode(',', $this->userRoles), -1, $cfg->cookie->path ?? '/'));
        }
    }

    public function checkCreateRights(): void {
        if (!$this->isAdmin && !$this->isCreator) {
            $this->denyAccess(RC::$config->accessControl->create->allowedRoles);
        }
    }

    public function checkAccessRights(int $resId, string $privilege,
                                      bool $metadataRead, bool $deny = true): void {
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
            if ($deny) {
                $this->denyAccess($allowed);
            } else {
                throw new RepoException('Unauthorized', $this->isPublic() ? 401 : 403);
            }
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

    public function isPublic(): bool {
        return $this->isPublic;
    }

    /**
     * 
     * @param array<string> $allowed
     * @return void
     * @throws RepoException
     */
    public function denyAccess(array $allowed): void {
        RC::$log->debug(json_encode(['roles' => $this->userRoles, 'allowed' => $allowed]));

        $cookieHeader = [];
        $cookieName   = RC::$config->accessControl->cookie->name ?? '';
        if (!empty($cookieName)) {
            $cookieHeader['set-cookie'] = $this->getCookieHeaderValue($cookieName, '', 0, RC::$config->accessControl->cookie->path ?? '/');
        }

        if ($this->isPublic()) {
            $resp = $this->controller->advertise();
            if ($resp !== null) {
                $headers = array_merge($resp->getHeaders(), $cookieHeader);
                throw new RepoException((string) $resp->getBody(), $resp->getStatusCode(), headers: $headers);
            }
        }
        throw new RepoException('Forbidden', 403, headers: $cookieHeader);
    }

    public function logout(string $redirectUrl = ''): void {
        $resp = $this->controller->logout($redirectUrl);
        if ($resp === null) {
            throw new RepoException('', 201);
        }
        $headers    = $resp->getHeaders();
        $cookieName = RC::$config->accessControl->cookie->name ?? '';
        if (!empty($cookieName)) {
            $headers['set-cookie'] = $this->getCookieHeaderValue($cookieName, '', 0, RC::$config->accessControl->cookie->path ?? '/');
        }
        throw new RepoException((string) $resp->getBody(), $resp->getStatusCode(), headers: $headers);
    }

    private function getCookieHeaderValue(string $name, string $value = '',
                                          int $expires = -1, string $path = ''): string {
        $cookie = rawurlencode($name) . '=' . rawurlencode($value);
        if ($expires >= 0) {
            $cookie .= '; max-Age=' . $expires;
        }
        if (!empty($path)) {
            $cookie .= '; path=' . $path;
        }
        return $cookie;
    }
}
