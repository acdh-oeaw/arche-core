BEGIN;

CREATE OR REPLACE FUNCTION get_relatives_metadata(
    res_id bigint, 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    neighbors bool default true,
    reverse int default 0
) RETURNS SETOF metadata_view LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH ids AS (
        SELECT * FROM get_relatives(res_id, rel_prop, max_depth_up, max_depth_down, neighbors, reverse > 0)
    )
    SELECT id, property, type, lang, value
    FROM metadata JOIN ids USING (id)
  UNION
    SELECT id, null::text AS property, 'ID'::text AS type, null::text AS lang, ids AS value
    FROM identifiers JOIN ids USING (id)
  UNION
    SELECT id, property, 'REL'::text AS type, null::text AS lang, target_id::text AS value
    FROM relations r JOIN ids USING (id)
  UNION
    SELECT r.id, property, 'REL'::text AS type, null::text AS lang, target_id::text AS value
    FROM relations r JOIN ids ON reverse = -1 AND r.target_id = ids.id
  ;
$$;

COMMIT;
