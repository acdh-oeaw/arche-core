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

use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Description of RepoException
 *
 * @author zozlak
 */
class RepoException extends RepoLibException {

    /**
     * HTTP response headers to be set
     * @var array<string, string|array<string>>
     */
    private array $headers;

    /**
     * 
     * @param array<string, string|array<string>> $headers
     */
    public function __construct(string $message = "", int $code = 0,
                                \Throwable | null $previous = null,
                                array $headers = []) {
        parent::__construct($message, $code, $previous);
        $this->headers = $headers;
    }

    /**
     * 
     * @return array<string, string|array<string>>
     */
    public function getHeaders(): array {
        return $this->headers;
    }
}
