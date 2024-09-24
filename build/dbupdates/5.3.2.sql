BEGIN;
DROP INDEX full_text_search_iid_index;
DROP INDEX full_text_search_mid_index;
CREATE INDEX full_text_search_id_index ON full_text_search USING btree (id) WHERE id IS NOT NULL;
CREATE INDEX full_text_search_iid_index ON full_text_search USING btree (iid) WHERE iid IS NOT NULL;
CREATE INDEX full_text_search_mid_index ON full_text_search USING btree (mid) WHERE mid IS NOT NULL;
COMMIT;
