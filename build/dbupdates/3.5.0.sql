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

DROP FUNCTION get_neighbors_metadata; -- same can be done with get_relatives_metadata(id, null, 0, 0, true, true)
DROP FUNCTION get_relatives_metadata;
DROP FUNCTION get_relatives;

CREATE OR REPLACE FUNCTION get_relatives_metadata(
    res_id bigint, 
    rel_prop text, 
    max_depth_up integer DEFAULT 999999, 
    max_depth_down integer default -999999, 
    neighbors bool default true,
    reverse bool default false
) RETURNS SETOF metadata_view LANGUAGE sql STABLE AS $$
    WITH ids AS (
        SELECT * FROM get_relatives(res_id, rel_prop, max_depth_up, max_depth_down, neighbors, reverse)
    )
    SELECT id, property, type, lang, value
    FROM metadata JOIN ids USING (id)
  UNION
    SELECT id, null::text AS property, 'ID'::text AS type, null::text AS lang, ids AS value
    FROM identifiers JOIN ids USING (id)
  UNION
    SELECT id, property, 'REL'::text AS type, null::text AS lang, target_id::text AS value
    FROM relations r JOIN ids USING (id)
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
) RETURNS SETOF record LANGUAGE sql STABLE AS $$
    WITH RECURSIVE ids(id, n, m) AS (
        SELECT res_id, 0, ARRAY[res_id] FROM resources WHERE id = res_id AND state = 'active'
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
            WHERE target_id = res_id AND reverse AND state = 'active'
        ) reverse USING (id)
  ;
$$;

COMMIT;
