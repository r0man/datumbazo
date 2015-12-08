-- Revert create-twitter-tweets-users

BEGIN;

DROP TABLE "twitter"."tweets-users";

COMMIT;
