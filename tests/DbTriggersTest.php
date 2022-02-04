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

namespace acdhOeaw\arche\core\tests;

use PDO;
use acdhOeaw\arche\lib\Config;

/**
 * Description of DbTriggersTest
 *
 * @author zozlak
 */
class DbTriggersTest extends \PHPUnit\Framework\TestCase {

    static private PDO $pdo;

    static public function setUpBeforeClass(): void {
        $cfg       = Config::fromYaml(__DIR__ . '/../config.yaml');
        self::$pdo = new PDO($cfg->dbConn->admin);
    }

    private function q(string $query): null | int {
        $stmt = self::$pdo->query($query);
        return $stmt === false ? null : (int) $stmt->fetchColumn();
    }

    public function setUp(): void {
        self::$pdo->query("TRUNCATE transactions CASCADE");
        self::$pdo->query("INSERT INTO resources (id) VALUES (-1), (-2)");
        self::$pdo->query("INSERT INTO identifiers (ids, id) VALUES ('-1', -1), ('-2', -2)");
        self::$pdo->query("INSERT INTO relations (id, target_id, property) VALUES (-1, -2, 'objProp')");
        self::$pdo->query("INSERT INTO spatial_search (id, geom) VALUES (-1, st_setsrid(st_point(11, 22), 4326)::geography)");
        self::$pdo->query("INSERT INTO full_text_search (id, segments, raw) VALUES (-1, to_tsvector('simple', 'binary'), 'binary')");
        self::$pdo->query("INSERT INTO metadata (id, property, type, lang, value) VALUES (-1, 'dataProp', 'string', '', 'literal')");
        self::$pdo->query("INSERT INTO metadata (id, property, type, lang, value) VALUES (-1, 'dataProp', 'GEOM', '', 'POINT(1 2)')");
    }

    public function testOnInsert(): void {
        $pdo = self::$pdo;
        // metadata propagate to spatial_search and full_text_search
        $this->assertEquals(2, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -1"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -1 AND raw = '-1'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -2 AND raw = '-2'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search JOIN metadata m USING (mid) WHERE m.id = -1"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM metadata_history"));
    }

    public function testOnUpdate(): void {
        $pdo = self::$pdo;

        // resource id change propagates to relations.target_id and metadata_history
        $pdo->query("UPDATE resources SET id = -4 WHERE id = -2");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM relations WHERE id = -1 AND target_id = -4"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'objProp' AND value = '-2'"));

        // resource id change propagates to all tables
        $pdo->query("UPDATE resources SET id = -3 WHERE id = -1");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM identifiers WHERE id = -3 AND ids = '-1'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM relations WHERE id = -3 AND target_id = -4"));
        $this->assertEquals(2, $this->q("SELECT count(*) FROM metadata WHERE id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE id = -3"));
        $this->assertEquals(2, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -1"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -3 AND raw = '-1'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search WHERE id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search JOIN metadata m USING (mid) WHERE m.id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -3 AND property = 'objProp' AND value = '-2'"));

        // identifier change propagates to metadata_history and full_text_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("UPDATE identifiers SET ids = '-3' WHERE id = -3");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -3 AND property = 'ID' AND value = '-1'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE iid = -3 AND raw = '-3'"));

        // relation change propagates to metadata_history
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("UPDATE relations SET target_id = -3 WHERE target_id = -4");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -3 AND property = 'objProp' AND value = '-4'"));

        // metadata change propagates to metadata_history, full_text_search and spatial_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("UPDATE metadata SET value = 'bar baz' WHERE type <> 'GEOM'");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -3 AND property = 'dataProp' AND value = 'literal'"));
        $this->assertEquals(2, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3 AND raw = 'bar baz'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3 AND raw = 'POINT(1 2)'"));
        $pdo->query("UPDATE metadata SET value = 'POINT(-1 -2)' WHERE type = 'GEOM'");
        $this->assertEquals(2, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -3 AND property = 'dataProp' AND value = 'POINT(1 2)'"));
        $this->assertEquals(2, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3 AND raw = 'bar baz'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search JOIN metadata m USING (mid) WHERE m.id = -3 AND raw = 'POINT(-1 -2)'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search JOIN metadata m USING (mid) WHERE m.id = -3"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search JOIN metadata m USING (mid) WHERE m.id = -3 AND geom = st_setsrid(st_point(-1, -2), 4326)::geography"));
    }

    public function testOnDeleteResource(): void {
        $pdo = self::$pdo;

        $pdo->query("INSERT INTO metadata_history (id, property, type, lang, value) VALUES (-1, 'prop', 'type', '', 'val')");
        $pdo->query("DELETE FROM resources WHERE id = -1");
        $this->assertEquals(0, $this->q("SELECT count(*) FROM identifiers WHERE id = -1"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM relations"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM metadata"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE iid <> -2"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM spatial_search"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM metadata_history"));
    }

    public function testOnDeleteOther(): void {
        $pdo = self::$pdo;

        // identifier delete propagates to metadata_history and full_text_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("DELETE FROM identifiers WHERE id = -1");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'ID' AND value = '-1'"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE iid <> -2"));

        // relation delete propagates to metadata_history
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("DELETE FROM relations WHERE id = -1");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'objProp' AND value = '-2'"));

        // metadata delete propagates to metadata_history, full_text_search and spatial_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("DELETE FROM metadata WHERE id = -1 AND type <> 'GEOM'");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'dataProp' AND value = 'literal'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE id IS NOT NULL"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE mid IS NOT NULL"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE mid IS NOT NULL AND raw <> 'POINT(1, 2)'"));
        $this->assertEquals(2, $this->q("SELECT count(*) FROM spatial_search"));
        $pdo->query("DELETE FROM metadata WHERE id = -1 AND type = 'GEOM'");
        $this->assertEquals(2, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'dataProp' AND value = 'POINT(1 2)'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE id IS NOT NULL"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE mid IS NOT NULL"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search WHERE id IS NOT NULL"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM spatial_search WHERE mid IS NOT NULL"));
    }

    public function testOnTruncateResources(): void {
        $pdo = self::$pdo;

        $pdo->query("INSERT INTO metadata_history (id, property, type, lang, value) VALUES (-1, 'prop', 'type', '', 'val')");
        $pdo->query("TRUNCATE resources CASCADE");
        $this->assertEquals(0, $this->q("SELECT count(*) FROM identifiers"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM relations"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM metadata"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM spatial_search"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM metadata_history"));
    }

    public function testOnTruncateOther(): void {
        $pdo = self::$pdo;

        // truncate identifiers propagates to metadata_history and full_text_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("TRUNCATE identifiers");
        $this->assertEquals(2, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'ID' AND value = '-1'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -2 AND property = 'ID' AND value = '-2'"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE iid IS NOT NULL"));

        // truncate relations propagates to metadata_history
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("TRUNCATE relations");
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'objProp' AND value = '-2'"));

        // truncate metadata propagates to metadata_history, full_text_search and spatial_search
        $pdo->query("TRUNCATE metadata_history");
        $pdo->query("TRUNCATE metadata");
        $pdo->query("DELETE FROM metadata WHERE id = -1 AND type = 'GEOM'");
        $this->assertEquals(2, $this->q("SELECT count(*) FROM metadata_history"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'dataProp' AND value = 'literal'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM metadata_history WHERE id = -1 AND property = 'dataProp' AND value = 'POINT(1 2)'"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM full_text_search WHERE id IS NOT NULL"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM full_text_search WHERE mid IS NOT NULL"));
        $this->assertEquals(1, $this->q("SELECT count(*) FROM spatial_search WHERE id IS NOT NULL"));
        $this->assertEquals(0, $this->q("SELECT count(*) FROM spatial_search WHERE mid IS NOT NULL"));
    }
}
