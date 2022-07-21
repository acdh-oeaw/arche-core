BEGIN;

ALTER TEXT SEARCH CONFIGURATION simple ALTER MAPPING FOR protocol WITH simple;

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain3() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM full_text_search WHERE (iid, raw) IN (SELECT id, ids FROM allold);
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO full_text_search (iid, segments, raw)
            SELECT id, to_tsvector('simple', ids), ids
            FROM allnew;
    END IF;
    RETURN NULL;
END;
$$;

UPDATE full_text_search SET segments = to_tsvector('simple', raw) WHERE iid IS NOT NULL;

COMMIT;
