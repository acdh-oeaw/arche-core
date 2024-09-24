BEGIN;
CREATE INDEX full_text_search_id_index ON full_text_search USING btree (id);
COMMIT;
