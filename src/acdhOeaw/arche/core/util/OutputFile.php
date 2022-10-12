<?php

/*
 * The MIT License
 *
 * Copyright 2022 zozlak.
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

use acdhOeaw\arche\core\RepoException;

/**
 * Outputs a file to a client optionally honoring the requested ranges.
 *
 * @author zozlak
 */
class OutputFile {

    const CHUNK_SIZE = 10485760; // 10 MB

    private string $path;

    /**
     * 
     * @var array<int, array<string, mixed>>
     */
    private array $ranges;
    private ?string $contentType;

    /**
     * 
     * @param string $path
     * @param array<int, array<string, mixed>>|null $ranges
     * @param string|null $contentType
     */
    public function __construct(string $path, ?array $ranges,
                                ?string $contentType) {
        $this->path = $path;
        if ($ranges !== null) {
            $this->ranges = $ranges;
        }
        $this->contentType = $contentType;
    }

    public function sendOutput(): void {
        $size = filesize($this->path);
        if (!isset($this->ranges)) {
            if (!empty($this->contentType)) {
                header("Content-Type: $this->contentType");
            }
            header("Content-Length: $size");
            readfile($this->path);
        } else {
            foreach ($this->ranges as $i) {
                if ($i['to'] >= $size) {
                    throw new RepoException('Range Not Satisfiable ', 416);
                }
            }

            http_response_code(206);

            $boundary    = '';
            $contentType = '';
            if (!empty($this->contentType)) {
                $contentType = "Content-Type: $this->contentType";
            }
            if (count($this->ranges) == 1) {
                header("$contentType");
                header("Content-Length: " . ($this->ranges[0]['to'] - $this->ranges[0]['from'] + 1));
            } else {
                $boundary = "--" . $this->ranges[0]['boundary'];
                $length   = $size + count($this->ranges) * (2 + strlen($contentType) + 2 + strlen($boundary) + 2) + 2;
                foreach ($this->ranges as &$i) {
                    $i['Content-Range'] = "Content-Range: bytes " . $i['from'] . "-" . $i['to'] . "/$size";
                    $length             += strlen($i['Content-Range']) + 2;
                }
                unset($i);
                header("Content-Type: multipart/byteranges; boundary=" . $this->ranges[0]['boundary']);
                header("Content-Length: $length");
            }

            $fh = fopen($this->path, 'r') ?: throw new RepoException("Failed to open $this->path");
            foreach ($this->ranges as $i) {
                echo count($this->ranges) > 1 ? "$boundary\r\n$contentType\r\n" . $i['Content-Range'] . "\r\n\r\n" : "";
                $pos = $i['from'];
                fseek($fh, $pos);
                while ($pos < $i['to']) {
                    echo fread($fh, min(self::CHUNK_SIZE, $i['to'] - $pos + 1));
                    $pos += self::CHUNK_SIZE;
                }
                echo count($this->ranges) > 1 ? "\r\n" : "";
            }
            fclose($fh);
            if (count($this->ranges) > 1) {
                echo "$boundary--\r\n";
            }
        }
    }
}
