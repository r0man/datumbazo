-- Revert create-twitter-users-table

BEGIN;

DROP TABLE twitter.users;

COMMIT;
