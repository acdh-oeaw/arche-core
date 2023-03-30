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

namespace acdhOeaw\arche\core\util;

/**
 * Provides SQL code for extracting geometries from various spatial formats
 * (geoJSON, KML, GML, raster images)
 *
 * @author zozlak
 */
class Spatial implements SpatialInterface {

    /**
     * Extracts union of all geometry properties no matter of their location in
     * a geoJSON.
     * 
     * @return Spatial
     */
    static public function fromGeojson(): Spatial {
        return new self(
            "
            SELECT st_union(geom) AS geom 
            FROM (
                SELECT st_geomfromgeojson(jsonb_path_query(?::jsonb, 'strict $.**.geometry')) AS geom
            ) t
            ",
            false
        );
    }

    /**
     * Extracts union of all kml:Point, kml:LineString, kml:Polygon from a KML
     * XML document.
     * 
     * It's worth noting this provides an implicit support for kml:MultiGeometry
     * as well.
     * 
     * @return Spatial
     */
    static public function fromKml(): Spatial {
        return new self(
            "
            SELECT st_union(st_geomfromkml(geom::text)) AS geom
            FROM
                unnest(xpath(
                    '//kml:Point|//kml:LineString|//kml:Polygon', 
                    ?::xml, 
                    ARRAY[ARRAY['kml', 'http://www.opengis.net/kml/2.2']]
                )) AS geom
            ",
            false
        );
    }

    /**
     * Extracts union of all gml:Point, gml:LineString and gml:Polygon from a
     * GML document.
     * 
     * @return Spatial
     */
    static public function fromGml(): Spatial {
        return new self(
            "
            SELECT st_union(st_geomfromgml(geom::text)) AS geom
            FROM
                unnest(xpath(
                    '//gml:Point|//gml:LineString|//gml:Polygon',
                    ?::xml,
                    ARRAY[ARRAY['gml', 'http://www.opengis.net/gml']]
                )) AS geom
            ",
            false
        );
    }

    /**
     * Extracts convex hull of a GDAL-supported raster excluding nodata pixels.
     * 
     * For multiple band rasters all bands are considered..
     * 
     * NULL is returned if the raster projection SRID is unknown.
     * 
     * @return Spatial
     */
    static public function fromRaster(): Spatial {
        return new self(
            "
            SELECT CASE st_srid(geom) > 0 WHEN true THEN geom ELSE null END AS geom 
            FROM st_minconvexhull(st_fromgdalraster(?)) AS geom
            ",
            true
        );
    }

    private string $query;
    private bool $binary;

    public function __construct(string $query, bool $binary) {
        $this->query  = $query;
        $this->binary = $binary;
    }

    public function getSqlQuery(): string {
        return $this->query;
    }

    public function isInputBinary(): bool {
        return $this->binary;
    }
}
