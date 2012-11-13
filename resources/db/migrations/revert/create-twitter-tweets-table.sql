-- Revert create-twitter-tweets-table

BEGIN;

DROP TABLE twitter.tweets;

COMMIT;
