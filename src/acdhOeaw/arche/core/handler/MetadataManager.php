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

namespace acdhOeaw\arche\core\handler;

use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use quickRdf\NamedNode;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\util\Triple;

/**
 * Description of MetadataManager
 *
 * @author zozlak
 */
class MetadataManager {

    static public function manage(int $id, DatasetNode $meta, ?string $path): DatasetNode {
        foreach (RC::$config->metadataManager->fixed as $p => $vs) {
            $p = DF::namedNode($p);
            foreach ($vs as $v) {
                self::addMetaValue($meta, $p, new Triple($v));
            }
        }
        foreach (RC::$config->metadataManager->default as $p => $vs) {
            $p = DF::namedNode($p);
            if ($meta->none(new PT($p))) {
                foreach ($vs as $v) {
                    self::addMetaValue($meta, $p, new Triple($v));
                }
            }
        }
        foreach (RC::$config->metadataManager->forbidden as $p) {
            $meta->delete(new PT($p));
        }
        foreach (RC::$config->metadataManager->copying as $sp => $tp) {
            $sp   = DF::namedNode($sp);
            $tp   = DF::namedNode($tp);
            $node = $meta->getNode();
            foreach ($meta->getIterator(new PT($sp)) as $triple) {
                $meta->add(DF::quad($node, $tp, $triple->getObject()));
            }
        }
        return $meta;
    }

    static private function addMetaValue(DatasetNode $meta, NamedNode $p,
                                         Triple $v): void {
        if (isset($v->uri)) {
            $meta->add(DF::quadNoSubject($p, DF::namedNode($v->uri)));
        } else {
            $literal = DF::literal((string) $v->value, $v->lang ?? null, $v->type ?? null);
            $meta->add(DF::quadNoSubject($p, $literal));
        }
    }
}
