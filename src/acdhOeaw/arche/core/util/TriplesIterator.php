<?php

/*
 * The MIT License
 *
 * Copyright 2023 zozlak.
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

namespace acdhOeaw\arche\core\util;

use PDOStatement;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\RepoException;
use rdfInterface\QuadInterface;
use rdfInterface\QuadIteratorInterface;
use rdfInterface\TermInterface;
use simpleRdf\DataFactory as DF;

/**
 * rdfInterface\QuadIteratorInterface wrapper for metadata_view table rows.
 * 
 * Supports caching.
 *
 * @author zozlak
 */
class TriplesIterator implements QuadIteratorInterface {

    private PDOStatement $query;
    private string $baseUrl;
    private string $idProp;
    private int $cacheSize;
    private int $n;
    private QuadInterface $triple;

    /**
     * 
     * @var array<QuadInterface>
     */
    private array $cache = [];

    public function __construct(PDOStatement $query, string $baseUrl,
                                string $idProp, int $cacheSize = 0) {
        $this->query     = $query;
        $this->baseUrl   = $baseUrl;
        $this->idProp    = $idProp;
        $this->cacheSize = $cacheSize;
        $this->n         = -1;
        $this->next();
    }

    public function current(): QuadInterface | null {
        return $this->triple ?? null;
    }

    public function key(): mixed {
        return isset($this->triple) ? $this->n : null;
    }

    public function next(): void {
        $this->n++;
        if (isset($this->cache[$this->n])) {
            $this->triple = $this->cache[$this->n];
        } else {
            $triple = $this->query->fetchObject(Triple::class);
            if ($triple instanceof Triple) {
                $sbj          = DF::namedNode($this->baseUrl . $triple->id);
                $prop         = DF::namedNode((string) ($triple->property === 'ID' ? $this->idProp : $triple->property));
                $obj          = $this->getObject($triple);
                $this->triple = DF::quad($sbj, $prop, $obj);
                if ($this->n < $this->cacheSize) {
                    $this->cache[$this->n] = $this->triple;
                }
            } else {
                unset($this->triple);
            }
        }
    }

    public function rewind(): void {
        if ($this->n < $this->cacheSize) {
            $this->n = -1;
            $this->next();
        } else {
            throw new RepoException("Can't rewind the iterator");
        }
    }

    public function valid(): bool {
        return isset($this->triple);
    }

    public function sort(): void {
        $subjects = [];
        foreach ($this->cache as $triple) {
            $subjects[$triple->getSubject()->getValue()][$triple->getPredicate()->getValue()][] = $triple->getObject();
        }
        $this->cache = [];
        foreach ($subjects as $sbj => $properties) {
            $sbj = DF::namedNode($sbj);
            foreach ($properties as $prop => $objects) {
                $prop = DF::namedNode($prop);
                foreach ($objects as $obj) {
                    $this->cache[] = DF::quad($sbj, $prop, $obj);
                }
            }
        }
    }

    private function getObject(Triple $triple): TermInterface {
        static $nonLiteralTypes = ['ID', 'REL', 'URI'];

        if (in_array($triple->type, $nonLiteralTypes)) {
            if ($triple->type === 'REL') {
                return DF::namedNode($this->baseUrl . $triple->value);
            } else {
                return DF::namedNode((string) $triple->value);
            }
        } else {
            if ($triple->type === 'GEOM') {
                $triple->type = RDF::XSD_STRING;
            }
            return DF::literal((string) $triple->value, $triple->lang, $triple->type);
        }
    }
}
