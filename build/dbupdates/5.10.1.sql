BEGIN;

CREATE OR REPLACE FUNCTION get_allowed_resources(acl_prop text, roles json)
RETURNS TABLE(id bigint) LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT DISTINCT id
    FROM metadata
    WHERE property = acl_prop AND value IN (SELECT json_array_elements_text(roles));
$$;

CREATE OR REPLACE FUNCTION get_resource_roles(read_prop text, write_prop text)
RETURNS TABLE(id bigint, role text, privilege text) LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT id, value, 'read'
    FROM resources JOIN metadata USING (id)
    WHERE property = read_prop
  UNION
    SELECT id, value, 'write'
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
    RAISE NOTICE 'Checks ended - removing constraints';
    ALTER TABLE spatial_search DROP CONSTRAINT spatial_search_id_fkey;
    ALTER TABLE full_text_search DROP CONSTRAINT full_text_search_id_fkey;
    ALTER TABLE identifiers DROP CONSTRAINT identifiers_id_fkey;
    ALTER TABLE metadata DROP CONSTRAINT metadata_id_fkey;
    ALTER TABLE relations DROP CONSTRAINT relations_id_fkey;
    ALTER TABLE relations DROP CONSTRAINT relations_target_id_fkey;
    DROP TRIGGER full_text_search_identifiers_delete_trigger ON identifiers;
    DROP TRIGGER metadata_history_identifiers_delete_trigger ON identifiers;
    DROP TRIGGER metadata_history_relations_delete_trigger ON relations;
    DROP TRIGGER full_text_search_metadata_delete_trigger ON metadata;
    DROP TRIGGER metadata_history_metadata_delete_trigger ON metadata;
    DROP TRIGGER spatial_search_metadata_delete_trigger ON metadata;
    DROP TRIGGER metadata_history_delete_trigger ON resources;
    RAISE NOTICE 'Constraints removed - deleting';

    DELETE FROM spatial_search WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM full_text_search WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM identifiers WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM relations WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM metadata WHERE id IN (SELECT id FROM __resToDel);
    DELETE FROM resources WHERE id IN (SELECT id FROM __resToDel);

    RAISE NOTICE 'Deleted - restoring constraints';
    ALTER TABLE relations ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE relations ADD FOREIGN KEY (target_id) REFERENCES resources(id) DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE metadata ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE identifiers ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE full_text_search ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    ALTER TABLE spatial_search ADD FOREIGN KEY (id) REFERENCES resources(id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
    CREATE TRIGGER full_text_search_identifiers_delete_trigger AFTER DELETE ON identifiers REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain3();
    CREATE TRIGGER metadata_history_identifiers_delete_trigger AFTER DELETE ON identifiers REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
    CREATE TRIGGER metadata_history_relations_delete_trigger AFTER DELETE ON relations REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
    CREATE TRIGGER full_text_search_metadata_delete_trigger AFTER DELETE ON metadata REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_full_text_search_maintain1();
    CREATE TRIGGER metadata_history_metadata_delete_trigger AFTER DELETE ON metadata REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_metadata_history_maintain1();
    CREATE TRIGGER spatial_search_metadata_delete_trigger AFTER DELETE ON metadata REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_spatial_search_maintain1();
    CREATE TRIGGER metadata_history_delete_trigger AFTER DELETE ON resources REFERENCING OLD TABLE AS allold FOR EACH STATEMENT EXECUTE FUNCTION tr_metadata_history_delete();

    RAISE NOTICE 'Deleted resources''s ids can be found in __restodel temporary table';
  END IF;
END;
$$ LANGUAGE plpgsql;

COMMIT:
