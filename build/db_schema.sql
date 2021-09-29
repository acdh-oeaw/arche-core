BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE OR REPLACE FUNCTION setgdal() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET postgis.gdal_enabled_drivers = ''ENABLE_ALL''';
    RETURN 0;
END;
$$;
SELECT setgdal();
DROP FUNCTION setgdal;

--
-- SEQUENCES
--

CREATE SEQUENCE ftsid_seq;
CREATE SEQUENCE id_seq;
CREATE SEQUENCE midh_seq;
CREATE SEQUENCE mid_seq;
CREATE SEQUENCE spid_seq;

--
-- TABLES & INDICES
--

CREATE TABLE transactions (
    transaction_id bigint NOT NULL PRIMARY KEY,
    started timestamp without time zone DEFAULT now() NOT NULL,
    last_request timestamp without time zone DEFAULT now() NOT NULL,
    state text DEFAULT 'active' NOT NULL,
    snapshot text NOT NULL,
    CONSTRAINT transactions_state_check CHECK ((state = ANY (ARRAY['active', 'commit', 'rollback'])))
);

CREATE TABLE users (
    user_id text NOT NULL PRIMARY KEY,
    data jsonb
);

CREATE TABLE resources (
    id bigint DEFAULT nextval('id_seq') NOT NULL PRIMARY KEY,
    transaction_id bigint REFERENCES transactions(transaction_id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,
    state text DEFAULT 'active' NOT NULL,
    CONSTRAINT resources_state_check CHECK ((state = ANY (ARRAY['active', 'tombstone', 'deleted'])))
);

CREATE TABLE identifiers (
    ids text NOT NULL PRIMARY KEY,
    id bigint NOT NULL REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX identifiers_id_index ON identifiers USING btree (id);

CREATE TABLE relations (
    id bigint NOT NULL REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    target_id bigint NOT NULL REFERENCES resources(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,
    property text NOT NULL,
    PRIMARY KEY (id, target_id, property)
);
CREATE INDEX relations_property_index ON relations USING btree (property);
CREATE INDEX relations_target_id_index ON relations USING btree (target_id);

CREATE TABLE metadata (
    mid bigint DEFAULT nextval('mid_seq') NOT NULL PRIMARY KEY,
    id bigint NOT NULL REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    property text NOT NULL,
    type text NOT NULL,
    lang text NOT NULL,
    value_n double precision,
    value_t timestamp without time zone,
    value text NOT NULL
);
CREATE INDEX metadata_id_index ON metadata USING btree (id);
CREATE INDEX metadata_property_index ON metadata USING btree (property);
CREATE INDEX metadata_value_index ON metadata USING btree (substring(value, 1, 1000));
CREATE INDEX metadata_value_gindex ON metadata USING gin (value, gin_trgm_ops);
CREATE INDEX metadata_value_n_index ON metadata USING btree (value_n);
CREATE INDEX metadata_value_t_index ON metadata USING btree (value_t);

CREATE TABLE full_text_search (
    ftsid bigint DEFAULT nextval('ftsid_seq') NOT NULL PRIMARY KEY,
    id bigint REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    mid bigint,
    segments tsvector NOT NULL,
    raw text,
    CHECK (id IS NULL OR mid IS NULL)
);
CREATE INDEX full_text_search_mid_index ON full_text_search USING btree (mid);
CREATE INDEX full_text_search_text_index ON full_text_search USING gin (segments);

CREATE TABLE spatial_search (
    spid bigint DEFAULT nextval('spid_seq') NOT NULL PRIMARY KEY,
    id bigint REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    mid bigint,
    geom geography NOT NULL,
    CHECK (id IS NULL OR mid IS NULL)
);
CREATE INDEX spatial_search_mid_index ON spatial_search USING btree (mid);
CREATE INDEX spatial_search_geom_index ON spatial_search USING gist (geom);

CREATE TABLE metadata_history (
    midh bigint DEFAULT nextval('midh_seq') NOT NULL PRIMARY KEY,
    date timestamp without time zone DEFAULT now() NOT NULL,
    id bigint NOT NULL,
    property text NOT NULL,
    type text NOT NULL,
    lang text NOT NULL,
    value text NOT NULL
);
CREATE INDEX metadata_history_date_index ON metadata_history USING btree (date);
CREATE INDEX metadata_history_id_index ON metadata_history USING btree (id);

--
-- VIEWS
-- 

CREATE OR REPLACE VIEW metadata_view AS
  SELECT id, property, type, lang, value FROM metadata
UNION
  SELECT id, NULL AS property, 'ID' AS type, NULL AS lang, ids AS value FROM identifiers
UNION
  SELECT id, property, 'REL' AS type, NULL AS lang, (r.target_id)::text AS value FROM relations r
;

--
-- UTILITY FUNCTIONS
-- 

CREATE OR REPLACE FUNCTION get_allowed_resources(acl_prop text, roles json) RETURNS TABLE(id bigint) LANGUAGE sql STABLE AS $$
    SELECT DISTINCT id
    FROM metadata
    WHERE property = acl_prop AND value IN (SELECT json_array_elements_text(roles));
$$;

CREATE OR REPLACE FUNCTION get_resource_roles(read_prop text, write_prop text) RETURNS TABLE(id bigint, role text, privilege text) LANGUAGE sql STABLE AS $$
    SELECT id, value, 'read' 
    FROM resources JOIN metadata USING (id) 
    WHERE property = read_prop 
  UNION
    SELECT id, value, 'read' 
    FROM resources JOIN metadata USING (id) 
    WHERE property = write_prop
  ;
$$;

CREATE OR REPLACE PROCEDURE delete_collection(resource_id bigint, rel_prop text) AS $$
DECLARE
  cnt int;
BEGIN
  DROP TABLE IF EXISTS __resToDel;
  CREATE TEMPORARY TABLE __resToDel AS SELECT * FROM get_relatives(resource_id, rel_prop, 999999, 0);

  DROP TABLE IF EXISTS __resConflict;
  CREATE TEMPORARY TABLE __resConflict AS
    SELECT *
    FROM relations r 
    WHERE 
      EXISTS (SELECT 1 FROM __resToDel WHERE r.target_id = id) 
      AND NOT EXISTS (SELECT 1 FROM __resToDel WHERE r.id = id);

  SELECT INTO cnt count(*) FROM __resConflict;
  IF cnt > 0 THEN
    RAISE NOTICE 'Aborting deletion as there are triples pointing to resources being removed - you can find them in the __resconflict temporary table';
  ELSE
    ALTER TABLE spatial_search DROP CONSTRAINT spatial_search_id_fkey;
    ALTER TABLE full_text_search DROP CONSTRAINT full_text_search_id_fkey;
    ALTER TABLE identifiers DROP CONSTRAINT identifiers_id_fkey;
    ALTER TABLE metadata DROP CONSTRAINT metadata_id_fkey;
    ALTER TABLE relations DROP CONSTRAINT relations_id_fkey;
    ALTER TABLE relations DROP CONSTRAINT relations_target_id_fkey;

    DELETE FROM spatial_search WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM full_text_search WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM identifiers WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM relations WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM metadata WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM resources WHERE id IN (SELECT id FROM __resToDel);

    ALTER TABLE relations ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE relations ADD FOREIGN KEY (target_id) REFERENCES resources(id) DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE metadata ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE identifiers ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE full_text_search ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE spatial_search ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

    RAISE NOTICE 'Deleted resources''s ids can be found in __restodel temporary table';
  END IF;
END;
$$ LANGUAGE plpgsql;

--
-- SEARCH FUNCTIONS
-- 

CREATE OR REPLACE FUNCTION get_neighbors_metadata(
    res_id bigint, 
    rel_prop text
) RETURNS SETOF metadata_view LANGUAGE sql AS $$
    WITH ids AS (
        SELECT id FROM resources WHERE id = res_id AND state = 'active'
      UNION
        SELECT id FROM relations WHERE (property = rel_prop OR rel_prop IS NULL) AND target_id = res_id
      UNION
        SELECT target_id AS id FROM relations WHERE id = res_id
    )
    SELECT id, property, type, lang, value
    FROM metadata JOIN ids USING (id)
  UNION
    SELECT id, null AS property, 'ID' AS type, null AS lang, ids AS value
    FROM identifiers JOIN ids USING (id)
  UNION
    SELECT id, property, 'REL' AS type, null AS lang, target_id::text AS value
    FROM relations r JOIN ids USING (id)
  ;
$$;

CREATE OR REPLACE FUNCTION get_relatives_metadata(
    res_id bigint, 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    neighbors bool default true,
    reverse bool default false
) RETURNS SETOF metadata_view LANGUAGE sql AS $$
    WITH 
        RECURSIVE ids(id, n, m) AS (
            SELECT res_id, 0, ARRAY[res_id] FROM resources WHERE id = res_id AND state = 'active'
          UNION
            SELECT
              CASE r.target_id WHEN ids.id THEN r.id ELSE r.target_id END,
              CASE r.target_id WHEN ids.id THEN ids.n + 1 ELSE ids.n - 1 END,
              CASE r.target_id WHEN ids.id THEN ARRAY[r.id] ELSE ARRAY[r.target_id] END || m
            FROM 
              relations r 
              JOIN ids ON (ids.n >= 0 AND ids.n < max_depth_up AND r.target_id = ids.id AND NOT r.id = ANY(ids.m)) OR (ids.n <= 0 AND ids.n > max_depth_down AND r.id = ids.id AND NOT r.target_id = ANY(ids.m))
            WHERE property = rel_prop
        ), 
        ids2 AS (
            SELECT id FROM ids
          UNION
            SELECT target_id AS id FROM ids JOIN relations r ON ids.id = r.id AND neighbors
          UNION
            SELECT id FROM relations WHERE target_id = res_id AND reverse
        )
    SELECT id, property, type, lang, value
    FROM metadata JOIN ids2 USING (id)
  UNION
    SELECT id, null AS property, 'ID' AS type, null AS lang, ids AS value
    FROM identifiers JOIN ids2 USING (id)
  UNION
    SELECT id, property, 'REL' AS type, null AS lang, target_id::text AS value
    FROM relations r JOIN ids2 USING (id)
  ;
$$;

CREATE OR REPLACE FUNCTION get_relatives(
    res_id bigint, 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    out id bigint, 
    out n int
) RETURNS SETOF record LANGUAGE sql AS $$
    WITH RECURSIVE ids(id, n, m) AS (
        SELECT res_id, 0, ARRAY[res_id] FROM resources WHERE id = res_id AND state = 'active'
      UNION
        SELECT
          CASE r.target_id WHEN ids.id THEN r.id ELSE r.target_id END,
          CASE r.target_id WHEN ids.id THEN ids.n + 1 ELSE ids.n - 1 END,
          CASE r.target_id WHEN ids.id THEN ARRAY[r.id] ELSE ARRAY[r.target_id] END || m
        FROM 
          relations r 
          JOIN ids ON (ids.n >= 0 AND ids.n < max_depth_up AND r.target_id = ids.id AND NOT r.id = ANY(ids.m)) OR (ids.n <= 0 AND ids.n > max_depth_down AND r.id = ids.id AND NOT r.target_id = ANY(ids.m))
        WHERE property = rel_prop OR rel_prop IS NULL
    )
    SELECT id, n FROM ids;
$$;

--
-- TRIGGERS REPLACING FOREIGN KEY metadata_history(id) REFERENCES resources(id) ON UPDATE CASCADE
-- 

-- UPDATE
CREATE OR REPLACE FUNCTION tr_metadata_history_id_fk() RETURNS TRIGGER language plpgsql AS $$
BEGIN
    UPDATE metadata_history SET id = NEW.id WHERE id = OLD.id;
    RETURN NULL;
END;
$$;
CREATE TRIGGER metadata_history_update_id_trigger AFTER UPDATE ON resources FOR EACH ROW WHEN (OLD.id <> NEW.id) EXECUTE FUNCTION tr_metadata_history_id_fk();
-- TRUNCATE
CREATE OR REPLACE FUNCTION tr_metadata_history_truncate() RETURNS TRIGGER language plpgsql AS $$
BEGIN
    TRUNCATE metadata_history;
    RETURN NULL;
END;
$$;
CREATE TRIGGER metadata_history_truncate_trigger AFTER TRUNCATE ON resources FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_truncate();
-- DELETE
CREATE OR REPLACE FUNCTION tr_metadata_history_delete() RETURNS TRIGGER language plpgsql AS $$
BEGIN
    DELETE FROM metadata_history WHERE id IN (SELECT id FROM allold);
    RETURN NULL;
END;
$$;
CREATE TRIGGER metadata_history_delete_trigger AFTER DELETE ON resources REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_delete();

-- 
-- TRIGGERS MAINTAINING full_text_search
--

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain1() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM full_text_search WHERE mid IN (SELECT mid FROM allold);
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO full_text_search (mid, segments, raw)
            SELECT mid, to_tsvector('simple', value), value
            FROM allnew;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER full_text_search_metadata_insert_trigger AFTER INSERT ON metadata REFERENCING                     NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain1();
CREATE TRIGGER full_text_search_metadata_update_trigger AFTER UPDATE ON metadata REFERENCING OLD TABLE AS allold NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain1();
CREATE TRIGGER full_text_search_metadata_delete_trigger AFTER DELETE ON metadata REFERENCING OLD TABLE AS allold                     FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain1();

CREATE OR REPLACE FUNCTION tr_full_text_search_maintain2() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM full_text_search WHERE mid IS NOT NULL;
    RETURN NULL;
END;
$$;
CREATE TRIGGER full_text_search_metadata_truncate_trigger AFTER TRUNCATE ON metadata FOR STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain2();

-- 
-- TRIGGERS MAINTAINING spatial_search
--

CREATE OR REPLACE FUNCTION tr_spatial_search_maintain1() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM spatial_search WHERE mid IN (SELECT mid FROM allold WHERE type = 'GEOM');
    END IF;
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
        INSERT INTO spatial_search (mid, geom)
            SELECT mid, st_geomfromtext(value, 4326)::geography
            FROM allnew
            WHERE type = 'GEOM';
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER spatial_search_metadata_insert_trigger AFTER INSERT ON metadata REFERENCING                     NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_spatial_search_maintain1();
CREATE TRIGGER spatial_search_metadata_update_trigger AFTER UPDATE ON metadata REFERENCING OLD TABLE AS allold NEW TABLE AS allnew FOR STATEMENT EXECUTE FUNCTION tr_spatial_search_maintain1();
CREATE TRIGGER spatial_search_metadata_delete_trigger AFTER DELETE ON metadata REFERENCING OLD TABLE AS allold                     FOR STATEMENT EXECUTE FUNCTION tr_spatial_search_maintain1();

CREATE OR REPLACE FUNCTION tr_spatial_search_maintain2() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM spatial_search WHERE mid IS NOT NULL;
    RETURN NULL;
END;
$$;
CREATE TRIGGER spatial_search_metadata_truncate_trigger AFTER TRUNCATE ON metadata FOR STATEMENT EXECUTE FUNCTION tr_spatial_search_maintain2();

-- 
-- TRIGGERS MAINTAINING metadata_history
--

CREATE OR REPLACE FUNCTION tr_metadata_history_maintain1() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'metadata' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id, property,  type, lang,     value FROM allold JOIN resources USING (id);
    ELSEIF TG_TABLE_NAME = 'relations' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id, property, 'URI',   '', target_id FROM allold JOIN resources USING (id);
    ELSEIF TG_TABLE_NAME = 'identifiers' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id,     'ID', 'URI',   '',       ids FROM allold JOIN resources USING (id);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER metadata_history_metadata_update_trigger    AFTER UPDATE ON metadata    REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
CREATE TRIGGER metadata_history_metadata_delete_trigger    AFTER DELETE ON metadata    REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
CREATE TRIGGER metadata_history_identifiers_update_trigger AFTER UPDATE ON identifiers REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
CREATE TRIGGER metadata_history_identifiers_delete_trigger AFTER DELETE ON identifiers REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
CREATE TRIGGER metadata_history_relations_update_trigger   AFTER UPDATE ON relations   REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
CREATE TRIGGER metadata_history_relations_delete_trigger   AFTER DELETE ON relations   REFERENCING OLD TABLE AS allold FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();

CREATE OR REPLACE FUNCTION tr_metadata_history_maintain2() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'metadata' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id, property,  type, lang,     value FROM metadata JOIN resources USING (id);
    ELSEIF TG_TABLE_NAME = 'relations' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id, property, 'URI',   '', target_id FROM relations JOIN resources USING (id);
    ELSEIF TG_TABLE_NAME = 'identifiers' THEN
        INSERT INTO metadata_history(id, property, type, lang, value) 
            SELECT id,     'ID', 'URI',   '',       ids FROM identifiers JOIN resources USING (id);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER metadata_history_metadata_truncate_trigger    BEFORE TRUNCATE ON metadata    FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain2();
CREATE TRIGGER metadata_history_identifiers_truncate_trigger BEFORE TRUNCATE ON identifiers FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain2();
CREATE TRIGGER metadata_history_relations_truncate_trigger   BEFORE TRUNCATE ON relations   FOR STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain2();

COMMIT;
