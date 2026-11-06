BEGIN;
ALTER TABLE resources ADD hash_check_date timestamp;
ALTER TABLE resources ADD hash_check_result text;
COMMIT;
