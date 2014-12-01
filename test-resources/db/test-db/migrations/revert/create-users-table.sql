-- Revert create-users-table

BEGIN;

DROP TABLE users;

COMMIT;
