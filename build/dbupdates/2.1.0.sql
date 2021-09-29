BEGIN;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX metadata_value_gindex ON metadata USING gin (value gin_trgm_ops);

-- fixed get_relatives() call to only incoude children
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

COMMIT;