BEGIN;

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain3() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM full_text_search WHERE (iid, raw) IN (SELECT id, ids FROM allold);
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO full_text_search (iid, segments, raw)
            SELECT id, to_tsvector('simple', ids), ids
            FROM allnew
          UNION
            SELECT id, to_tsvector('simple', replace(regexp_replace(lower(ids), '^https?://[^/]+/', ''), '/', ' ')), ids
            FROM allnew
            WHERE ids ~* '^\w+:(\/?\/?)[^\s]+';
    END IF;
    RETURN NULL;
END;
$$;

INSERT INTO full_text_search (iid, segments, raw) SELECT id, to_tsvector('simple', replace(regexp_replace(lower(ids), '^https?://[^/]+/', ''), '/', ' ')), ids FROM identifiers WHERE ids ~* '^\w+:(\/?\/?)[^\s]+';

COMMIT;
