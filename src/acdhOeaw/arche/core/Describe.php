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

use RuntimeException;
use zozlak\HttpAccept;
use Composer\InstalledVersions;
use acdhOeaw\arche\core\RestController as RC;
use function \GuzzleHttp\json_encode;

/**
 * Handles the /desribe endpoint
 *
 * @author zozlak
 */
class Describe {

    public function head(bool $get = false): void {
        $cfg = [
            'version' => InstalledVersions::getVersion('acdh-oeaw/arche-core'),
            'rest'    => [
                'headers'  => RC::$config->rest->headers,
                'urlBase'  => RC::$config->rest->urlBase,
                'pathBase' => RC::$config->rest->pathBase
            ],
            'schema'  => RC::$config->schema
        ];
        if (filter_input(\INPUT_GET, 'format') === 'application/json') {
            $format = 'application/json';
        } else {
            try {
                $format = HttpAccept::getBestMatch(['text/vnd.yaml', 'application/json'])->getFullType();
            } catch (RuntimeException $e) {
                $format = 'text/vnd.yaml';
            }
        }
        $response = match ($format) {
            'application/json' => json_encode($cfg),
            default => yaml_emit(json_decode(json_encode($cfg), true)),
        };
        RC::setHeader('Content-Size', (string) strlen($response));
        RC::setHeader('Content-Type', $format);
        if ($get) {
            RC::setOutput($response);
        }
    }

    public function get(): void {
        $this->head(true);
    }

    public function options(int $code = 200): void {
        http_response_code($code);
        header('Allow: HEAD, GET');
    }
}
