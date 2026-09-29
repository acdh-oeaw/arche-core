BEGIN;
CREATE OR REPLACE FUNCTION tr_spatial_search_maintain1() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM spatial_search WHERE mid IN (SELECT mid FROM allold WHERE type = 'GEOM');
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO spatial_search (mid, geom)
            SELECT mid, st_geomfromtext(replace(value, '+', ''), 4326)::geography
            FROM allnew
            WHERE type = 'GEOM';
    END IF;
    RETURN NULL;
END;
$$;
COMMIT;
