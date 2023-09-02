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

namespace acdhOeaw\arche\core\tests;

use DirectoryIterator;
use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration;
use PHPUnit\Event\TestRunner\ExecutionStarted;
use PHPUnit\Event\TestRunner\ExecutionFinished;
use PHPUnit\Event\TestRunner\ExecutionStartedSubscriber;
use PHPUnit\Event\TestRunner\ExecutionFinishedSubscriber;
use SebastianBergmann\CodeCoverage\CodeCoverage;
use SebastianBergmann\CodeCoverage\Filter;
use SebastianBergmann\CodeCoverage\Data\RawCodeCoverageData;
use SebastianBergmann\CodeCoverage\Report\Clover;
use SebastianBergmann\CodeCoverage\Report\Html\Facade as HtmlFacade;
use SebastianBergmann\CodeCoverage\Driver\XdebugDriver;
use acdhOeaw\arche\lib\Config;
use function \GuzzleHttp\json_decode;

/**
 * Description of CoverageGen
 *
 * @author zozlak
 */
class Bootstrap implements Extension {

    public function bootstrap(Configuration $configuration, Facade $facade,
                              ParameterCollection $parameters): void {

        $facade->registerSubscriber(new Init());
        $facade->registerSubscriber(new Coverage());
    }
}

class Init implements ExecutionStartedSubscriber {

    private Config $config;

    public function __construct() {
        $this->config = Config::fromYaml(__DIR__ . '/config.yaml');
    }

    public function notify(ExecutionStarted $event): void {
        $buildlogsDir = __DIR__ . '/../build/logs';
        system('rm -fR ' . escapeshellarg($buildlogsDir) . ' && mkdir ' . escapeshellarg($buildlogsDir));

        system('rm -fR ' . escapeshellarg($this->config->storage->dir) . ' && mkdir -p ' . escapeshellarg($this->config->storage->dir));
        system('rm -fR ' . escapeshellarg($this->config->storage->tmpDir) . ' && mkdir -p ' . escapeshellarg($this->config->storage->tmpDir));

        if (!file_exists(__DIR__ . '/data/baedeker.xml')) {
            file_put_contents(__DIR__ . '/data/baedeker.xml', file_get_contents('https://id.acdh.oeaw.ac.at/traveldigital/Corpus/Baedeker-Konstantinopel_und_Kleinasien_1905.xml?format=raw'));
        }

        if (file_exists($this->config->transactionController->logging->file)) {
            unlink($this->config->transactionController->logging->file);
        }
        if (file_exists($this->config->rest->logging->file)) {
            unlink($this->config->rest->logging->file);
        }

        if (!file_exists(dirname($this->config->transactionController->logging->file))) {
            system('mkdir -p ' . dirname($this->config->transactionController->logging->file));
        }
        if (!file_exists(dirname($this->config->rest->logging->file))) {
            system('mkdir -p ' . dirname($this->config->rest->logging->file));
        }
    }
}

class Coverage implements ExecutionFinishedSubscriber {

    public function notify(ExecutionFinished $event): void {
        $filter = new Filter();
        $filter->includeDirectory(__DIR__ . '/../src');
        $driver = new XdebugDriver($filter);
        $cc     = new CodeCoverage($driver, $filter);
        foreach (new DirectoryIterator(__DIR__ . '/../build/logs') as $i) {
            if ($i->getExtension() === 'json') {
                $data = (array) json_decode((string) file_get_contents($i->getPathname()), true);

                $data = RawCodeCoverageData::fromXdebugWithoutPathCoverage($data);
                $cc->append($data, '');
            }
        }
        $writer = new Clover();
        $writer->process($cc, __DIR__ . '/../build/logs/clover.xml');
        $writer = new HtmlFacade();
        $writer->process($cc, __DIR__ . '/../build/logs/');
    }
}
