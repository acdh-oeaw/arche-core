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

use quickRdf\DataFactory as DF;
use quickRdf\NamedNode;
use acdhOeaw\arche\lib\Config;

/**
 * Container for rdfInterface schema objects
 *
 * @author zozlak
 */
class Schema {

    static public function fromConfig(Config $cfg): self {
        $cfg    = $cfg->schema;
        $schema = new Schema();
        foreach (array_keys(get_class_vars($schema::class)) as $k) {
            $schema->$k = DF::namedNode($cfg->$k);
        }
        return $schema;
    }

    public NamedNode $id;
    public NamedNode $label;
    public NamedNode $parent;
    public NamedNode $creationDate;
    public NamedNode $creationUser;
    public NamedNode $modificationDate;
    public NamedNode $modificationUser;
    public NamedNode $binaryModificationDate;
    public NamedNode $binaryModificationUser;
    public NamedNode $delete;
    public NamedNode $fileName;
    public NamedNode $mime;
    public NamedNode $binarySize;
    public NamedNode $hash;
    public NamedNode $searchMatch;
}
