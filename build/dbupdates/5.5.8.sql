BEGIN;

CREATE INDEX full_text_search_raw_gindex ON full_text_search USING gin (raw gin_trgm_ops);

COMMIT;