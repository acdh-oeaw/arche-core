BEGIN;

DROP INDEX full_text_search_mid_index;
DROP INDEX full_text_search_text_index;
ALTER TABLE full_text_search DROP CONSTRAINT full_text_search_id_fkey;
ALTER TABLE full_text_search RENAME TO fts;

CREATE TABLE full_text_search (
    ftsid bigint DEFAULT nextval('ftsid_seq') NOT NULL PRIMARY KEY,
    id bigint REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    iid bigint,
    mid bigint,
    segments tsvector NOT NULL,
    raw text,
    CHECK ((id IS NULL)::int + (iid IS NULL)::int + (mid IS NULL)::int = 2)
);
CREATE INDEX full_text_search_iid_index ON full_text_search USING btree (iid);
CREATE INDEX full_text_search_mid_index ON full_text_search USING btree (mid);
CREATE INDEX full_text_search_text_index ON full_text_search USING gin (segments);

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain3() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM full_text_search WHERE (iid, raw) IN (SELECT id, ids FROM allold);
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO full_text_search (iid, segments, raw)
            SELECT id, to_tsvector('simple', regexp_replace(ids, '^([^:]*):/', '\1 ')), ids
            FROM allnew;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER full_text_search_identifiers_insert_trigger AFTER INSERT ON identifiers REFERENCING                     NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain3();
CREATE TRIGGER full_text_search_identifiers_update_trigger AFTER UPDATE ON identifiers REFERENCING OLD TABLE AS allold NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain3();
CREATE TRIGGER full_text_search_identifiers_delete_trigger AFTER DELETE ON identifiers REFERENCING OLD TABLE AS allold                     FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain3();

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain4() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM full_text_search WHERE iid IS NOT NULL;
    RETURN NULL;
END;
$$;
CREATE TRIGGER full_text_search_identifiers_truncate_trigger AFTER TRUNCATE ON identifiers FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain4();

INSERT INTO full_text_search SELECT ftsid, id, null, mid, segments, raw FROM fts;
DROP TABLE fts;

INSERT INTO full_text_search (iid, segments, raw) SELECT id, to_tsvector('simple', regexp_replace(ids, '^([^:]*):/', '\1 ')), ids FROM identifiers;

COMMIT;
