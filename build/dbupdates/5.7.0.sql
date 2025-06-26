BEGIN;

CREATE OR REPLACE FUNCTION get_relatives(
    res_ids bigint[],
    rel_prop text,
    max_depth_up integer DEFAULT 999999,
    max_depth_down integer default -999999,
    neighbors bool default false,
    reverse bool default false,
    out id bigint,
    out n int
) RETURNS SETOF record LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH RECURSIVE ids(id, n, m) AS (
        SELECT id, 0, ARRAY[id] FROM (SELECT unnest(res_ids) AS id) JOIN resources USING (id) WHERE state = 'active'
      UNION
        SELECT
          CASE r.target_id WHEN ids.id THEN r.id ELSE r.target_id END,
          CASE r.target_id WHEN ids.id THEN ids.n + 1 ELSE ids.n - 1 END,
          CASE r.target_id WHEN ids.id THEN ARRAY[r.id] ELSE ARRAY[r.target_id] END || m
        FROM
          relations r
          JOIN ids ON (max_depth_up > 0 AND ids.n >= 0 AND ids.n < max_depth_up AND r.target_id = ids.id AND NOT r.id = ANY(ids.m)) OR (max_depth_down < 0 AND ids.n <= 0 AND ids.n > max_depth_down AND r.id = ids.id AND NOT r.target_id = ANY(ids.m))
        WHERE property = rel_prop OR rel_prop IS NULL
    )
    SELECT id, n
    FROM
        ids
        FULL JOIN (
            SELECT DISTINCT target_id AS id
            FROM ids JOIN resources USING(id) JOIN relations r USING (id)
            WHERE neighbors AND state = 'active'
        ) neighbors USING (id)
        FULL JOIN (
            SELECT DISTINCT id
            FROM relations JOIN resources USING (id)
            WHERE target_id = ANY(res_ids) AND reverse AND state = 'active'
        ) reverse USING (id)
  ;
$$;

CREATE OR REPLACE FUNCTION get_relatives_metadata(
    res_ids bigint[], 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    neighbors bool default true,
    reverse int default 0
) RETURNS SETOF metadata_view LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH ids AS (
        SELECT * FROM get_relatives(res_ids, rel_prop, max_depth_up, max_depth_down, neighbors, reverse > 0)
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

CREATE OR REPLACE FUNCTION get_relatives(
    res_id bigint,
    rel_prop text,
    max_depth_up integer DEFAULT 999999,
    max_depth_down integer default -999999,
    neighbors bool default false,
    reverse bool default false,
    out id bigint,
    out n int
) RETURNS SETOF record LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT * FROM get_relatives(ARRAY[res_id], rel_prop, max_depth_up, max_depth_down, neighbors, reverse);
$$;

CREATE OR REPLACE FUNCTION get_relatives_metadata(
    res_id bigint, 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    neighbors bool default true,
    reverse int default 0
) RETURNS SETOF metadata_view LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT * FROM get_relatives_metadata(ARRAY[res_id], rel_prop, max_depth_up, max_depth_down, neighbors, reverse);
$$;

COMMIT;
