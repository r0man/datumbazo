-- Revert create-citext-extension

BEGIN;

DROP EXTENSION citext;

COMMIT;
