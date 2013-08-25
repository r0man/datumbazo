-- Revert create-twitter-tweets-users

BEGIN;

DROP TABLE twitter.tweets_users;

COMMIT;
