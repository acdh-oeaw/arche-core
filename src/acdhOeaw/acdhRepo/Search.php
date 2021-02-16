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

namespace acdhOeaw\acdhRepo;

use PDO;
use acdhOeaw\acdhRepo\RestController as RC;
use acdhOeaw\acdhRepoLib\RepoDb;
use acdhOeaw\acdhRepoLib\Schema;
use acdhOeaw\acdhRepoLib\SearchTerm;
use acdhOeaw\acdhRepoLib\SearchConfig;

/**
 * Description of Search
 *
 * @author zozlak
 */
class Search {

    /**
     *
     * @var \PDO
     */
    private $pdo;

    public function post(): void {
        $this->pdo = new PDO(RC::$config->dbConnStr->guest);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->query("SET application_name TO rest_search");

        $schema                         = new Schema(RC::$config->schema);
        $headers                        = new Schema(RC::$config->rest->headers);
        $nonRelProp                     = RC::$config->metadataManagment->nonRelationProperties;
        $repo                           = new RepoDb(RC::getBaseUrl(), $schema, $headers, $this->pdo, $nonRelProp, RC::$auth);
        $repo->setQueryLog(RC::$log);
        $config                         = SearchConfig::factory();
        $config->metadataMode           = RC::getRequestParameter('metadataReadMode') ?? RC::$config->rest->defaultMetadataSearchMode;
        $config->metadataParentProperty = RC::getRequestParameter('metadataParentProperty') ?? RC::$config->schema->parent;
        if (isset($_POST['sql'])) {
            $params = $_POST['sqlParam'] ?? [];
            $graph  = $repo->getGraphBySqlQuery($_POST['sql'], $params, $config);
        } else {
            $terms = [];
            for ($n = 0; isset($_POST['property'][$n]) || isset($_POST['value'][$n]) || isset($_POST['language'][$n]); $n++) {
                $terms[] = SearchTerm::factory($n);
            }
            $graph = $repo->getGraphBySearchTerms($terms, $config);
        }

        $meta   = new Metadata(0);
        $meta->loadFromGraph($graph);
        $format = $meta->outputHeaders();
        $meta->outputRdf($format);
    }

    public function get(): void {
        foreach ($_GET as $k => $v) {
            $_POST[$k] = $v;
        }
        $this->post();
    }

    public function options(int $code = 200): void {
        http_response_code($code);
        header('Allow: OPTIONS, HEAD, GET, POST');
    }

}
