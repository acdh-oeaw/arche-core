BEGIN;
ALTER TABLE resources ADD lock int;
CREATE INDEX resources_transaction_id_index ON resources(transaction_id) WHERE transaction_id IS NOT NULL;
CREATE INDEX resources_lock_index ON resources(lock) WHERE lock IS NOT NULL;
COMMIT;
