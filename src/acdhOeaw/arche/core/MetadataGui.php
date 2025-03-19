<?php

/*
 * The MIT License
 *
 * Copyright 2020 Austrian Centre for Digital Humanities.
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

use PDOStatement;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\util\Triple;

/**
 * Provides simple HTML serialization of a metadata triples set
 *
 * @author zozlak
 */
class MetadataGui {

    const NO_TYPE    = [RDF::XSD_STRING, RDF::RDF_LANG_STRING];
    const CHILD_PROP = 'Child resources';
    const TYPE_ID    = 'ID';
    const TYPE_REL   = 'REL';
    const TYPE_URI   = 'URI';
    const TMPL       = <<<TMPL
<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8"/>
        <title>%s</title>
        <style>
            body {font-family: monospace;}
            .n   {padding-left: 0rem; font-weight: normal;}
            .s   {padding-left: 0rem; font-weight: bold;}
            .p   {padding-left: 4rem; font-style: normal;}
            .o   {padding-left: 8rem; font-style: normal;}
            .tl  {font-style: italic;}
            .p > a {text-decoration: none; color: inherit;}
        </style>
    </head>
    <body>
        <h1>%s</h1>
TMPL;

    /**
     *
     * @var int
     */
    private $res;

    /**
     *
     * @var array<string, string>
     */
    private $nmsp;

    /**
     *
     * @var array<string, string>
     */
    private $titles;

    /**
     *
     * @var array<string, string>
     */
    private $properties;

    /**
     *
     * @var array<string, array<string, array<mixed>>>
     */
    private $data;

    /**
     * 
     * @var mixed
     */
    private $stream;

    /**
     * 
     * @param mixed $stream
     * @param PDOStatement $query
     * @param int $resId
     * @param string $preferredLang
     */
    public function __construct($stream, PDOStatement $query, int $resId,
                                string $preferredLang = 'en') {
        $this->stream = $stream;
        $baseUrl      = RC::getBaseUrl();
        $this->res    = $resId;
        $this->nmsp   = RC::$config->schema->namespaces ?? [];
        $matchProp    = RC::$schema->searchMatch->getValue();
        $idProp       = RC::$schema->id->getValue();
        $titleProp    = RC::$schema->label->getValue();
        $parentProp   = RC::$schema->parent->getValue();

        if ($resId > 0) {
            $matchFunc = function (Triple $t) use ($resId): bool {
                return $t->id === $resId;
            };
        } else {
            $matchFunc = function (Triple $t) use ($matchProp): bool {
                return $t->property === $matchProp;
            };
        }

        $this->titles = [];
        $this->data   = [];
        $matches      = [];
        while ($triple       = $query->fetchObject(Triple::class)) {
            $id = (string) $triple->id;
            if ($triple->type === self::TYPE_ID) {
                $triple->property = $idProp;
            }
            // triples sorted by sbj and property
            if (!isset($this->data[$id])) {
                $this->data[$id] = [];
            }
            if (!isset($this->data[$id][$triple->property])) {
                $this->data[$id][$triple->property] = [];
            }
            $this->data[$id][$triple->property][] = $triple;
            // global titles map
            $tid                                  = $baseUrl . $id;
            if ($triple->property === $titleProp && (!isset($this->titles[$tid]) || $this->titles[$tid]->lang !== $preferredLang)) {
                $this->titles[$tid] = $triple;
            }
            // resources to keep
            if ($matchFunc($triple)) {
                $matches[$id] = 1;
            }
            // global property list
            $this->properties[$triple->property] = '';
            // children
            if ($triple->property === $parentProp) {
                if (!isset($this->data[$triple->value])) {
                    $this->data[$triple->value] = [];
                }
                if (!isset($this->data[$triple->value][self::CHILD_PROP])) {
                    $this->data[$triple->value][self::CHILD_PROP] = [];
                }
                $t                                              = clone($triple);
                $t->value                                       = (string) $t->id;
                $this->data[$triple->value][self::CHILD_PROP][] = $t;
            }
        }
        foreach (array_diff(array_keys($this->data), array_keys($matches)) as $k) {
            unset($this->data[$k]);
        }

        $this->properties[$idProp]          = '';
        $this->properties[self::CHILD_PROP] = '';
        foreach ($this->properties as $k => &$v) {
            $v = $this->formatResource($k, false);
        }
        unset($v);

        foreach ($this->titles as $k => &$v) {
            $v = htmlspecialchars((string) $v->value);
        }
        unset($v);
    }

    public function output(): void {
        $baseUrl    = RC::getBaseUrl();
        $idProp     = RC::$schema->id->getValue();
        $titleProp  = RC::$schema->label->getValue();
        $parentProp = RC::$schema->parent->getValue();
        $skipProps  = [$idProp, $titleProp, $parentProp, self::CHILD_PROP];

        $title  = "$baseUrl$this->res";
        $header = $title;
        if ($this->res <= 0) {
            $title  = 'Search results';
            $header = count($this->data) . ' resource(s) found';
        }
        fwrite($this->stream, sprintf(self::TMPL, $title, $header));

        foreach ($this->data as $id => $props) {
            fwrite($this->stream, '        <a href="' . htmlspecialchars($baseUrl . $id) . '" class="s">' . $this->formatResource($baseUrl . $id) . "</a>\n");
            // title
            $this->outputProperty($props[$titleProp] ?? [], $titleProp, $baseUrl);
            // ids
            $this->outputProperty($props[$idProp] ?? [], $idProp, $baseUrl);
            // is part of
            $this->outputProperty($props[$parentProp] ?? [], $parentProp, $baseUrl);
            // all other but children
            $properties = array_diff(array_keys($props), $skipProps);
            sort($properties);
            foreach ($properties as $p) {
                $this->outputProperty($props[$p], $p, $baseUrl);
            }
            // children
            $this->outputProperty($props[self::CHILD_PROP] ?? [], self::CHILD_PROP, $baseUrl);
        }

        fwrite($this->stream, "    </body>\n</html>");
    }

    /**
     * 
     * @param array<Triple> $values
     * @param string $p
     * @param string $baseUrl
     * @return void
     */
    private function outputProperty(array $values, string $p, string $baseUrl): void {
        if (count($values) > 0) {
            $pHead = '<div class="p">';
            $pHead .= str_starts_with($p, 'http')?'<a href="' . htmlspecialchars($p) . '">':'';
            $pHead .= $this->properties[$p];
            $pHead .= str_starts_with($p, 'http')?'</a>':'';
            $pHead .= "</a></div>\n";
            fwrite($this->stream, $pHead);
            
            foreach ($values as $n => $t) {
                fwrite($this->stream, '<div class="o">' . $this->formatObject($t, $baseUrl) . '&nbsp;' . ($n + 1 === count($values) ? '.' : ',') . "</div>\n");
            }
        }
    }

    private function formatObject(Triple $o, string $baseUrl): string {
        if ($o->type === self::TYPE_ID || $o->type === self::TYPE_REL || $o->type === self::TYPE_URI) {
            $o->value ??= '';
            if ($o->type === self::TYPE_REL) {
                $o->value = $baseUrl . $o->value;
            }
            $href   = htmlspecialchars($o->value);
            $suffix = htmlspecialchars(substr($o->value, 0, strlen($baseUrl)) === $baseUrl ? '/metadata' : '');
            return sprintf('<a href="%s%s">%s</a>', $href, $suffix, $this->formatResource($o->value));
        } else {
            $l = htmlspecialchars(empty($o->lang) ? '' : ('@' . $o->lang));
            $t = empty($o->type) || in_array($o->type, self::NO_TYPE) ? '' : ('^^' . $this->formatResource($o->type, false));
            return sprintf('"%s"<span class="tl">%s</span>', htmlspecialchars($o->value), $l . $t);
        }
    }

    private function formatResource(string $res, bool $tryTitles = true): string {
        if ($tryTitles && isset($this->titles[$res])) {
            return $this->titles[$res];
        }
        foreach ($this->nmsp as $n => $u) {
            if (substr($res, 0, strlen($u)) === $u) {
                return $n . ':' . substr($res, strlen($u));
            }
        }
        return '&lt;' . htmlspecialchars($res) . '&gt;';
    }
}
