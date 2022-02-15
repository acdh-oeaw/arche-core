<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
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

use PDO;
use stdClass;
use acdhOeaw\arche\core\RestController as RC;
use zozlak\auth\usersDb\PdoDb;
use zozlak\auth\usersDb\UserUnknownException;
use zozlak\auth\authMethod\HttpBasic;

/**
 * Implements the REST API users management API
 *
 * @author zozlak
 */
class UserApi {

    /**
     * 
     * @var \zozlak\auth\usersDb\PdoDb
     */
    private $db;

    public function __construct() {
        $cfg      = RC::$config->accessControl;
        $this->db = new PdoDb($cfg->db->connStr, $cfg->db->table, $cfg->db->userCol, $cfg->db->dataCol);
    }

    public function put(string $user): void {
        if (!RC::$auth->isAdmin()) {
            throw new RepoException('Forbidden', 403);
        }

        try {
            $this->db->getUser($user);
            throw new RepoException('User already exists', 400);
        } catch (UserUnknownException $ex) {
            http_response_code(201);
            $this->db->putUser($user);
            $this->patch($user);
        }
    }

    public function get(string $user): void {
        if (!RC::$auth->isAdmin() && RC::$auth->getUserName() !== $user) {
            throw new RepoException('Forbidden', 403);
        }

        if (empty($user)) {
            $query = RC::$pdo->query("SELECT user_id FROM users ORDER BY user_id") ?: throw new RepoException("Query failed");
            $users = $query->fetchAll(PDO::FETCH_COLUMN) ?: [];
            $data  = [];
            foreach ($users as $user) {
                $data[] = $this->prepareUserData($this->db->getUser($user), $user);
            }
        } else {
            $data = $this->checkUserExists($user);
            $data = $this->prepareUserData($data, $user);
        }

        $data = json_encode($data) ?: throw new \RuntimeException("Can't serialise to JSON");
        RC::setOutput($data, 'application/json');
    }

    public function patch(string $user): void {
        $admin = RC::$auth->isAdmin();
        if (!$admin && RC::$auth->getUserName() !== $user) {
            throw new RepoException('Forbidden', 403);
        }
        $this->checkUserExists($user);

        parse_str((string) file_get_contents('php://input'), $post);
        $json    = json_decode((string) file_get_contents('php://input'));
        $newData = new stdClass();
        $fields  = $admin ? ['password', 'groups'] : ['password'];
        foreach ($fields as $i) {
            if (isset($_GET[$i])) {
                $newData->$i = $_GET[$i];
            } elseif (isset($post[$i])) {
                $newData->$i = $post[$i];
            } elseif (isset($json->$i)) {
                $newData->$i = $json->$i;
            }
        }
        if (isset($newData->password)) {
            $this->db->putUser($user, HttpBasic::pswdData($newData->password), true);
            unset($newData->password);
        }
        if (isset($newData->groups) && !is_array($newData->groups)) {
            $newData->groups = [$newData->groups];
        }
        $this->db->putUser($user, $newData, true);

        $this->get($user);
    }

    public function delete(string $user): void {
        if (!RC::$auth->isAdmin() && RC::$auth->getUserName() !== $user) {
            throw new RepoException('Forbidden', 403);
        }
        $this->checkUserExists($user);

        $this->db->deleteUser($user);
        http_response_code(204);
    }

    public function options(string $user, int $code = 200): void {
        http_response_code($code);
        header('Allow: OPTIONS, PUT, HEAD, GET, PATCH, DELETE');
    }

    private function checkUserExists(string $user): stdClass {
        try {
            return $this->db->getUser($user);
        } catch (UserUnknownException $ex) {
            throw new RepoException('No such user', 404);
        }
    }

    private function prepareUserData(object $data, string $user): object {
        $data           = (array) $data; // to avoid phpstan complains
        $data['userId'] = $user;
        if (!isset($data['groups'])) {
            $data['groups'] = [];
        }
        if (isset(RC::$config->accessControl->publicRole)) {
            $data['groups'][] = RC::$config->accessControl->publicRole;
        }
        unset($data['pswd']);
        return (object) $data;
    }
}
