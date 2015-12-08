-- Deploy create-twitter-tweets-users

BEGIN;

CREATE TABLE twitter."tweets-users" (
  "tweet-id" bigint NOT NULL REFERENCES twitter.tweets(id) ON DELETE CASCADE,
  "user-id" bigint NOT NULL REFERENCES twitter.users(id) ON DELETE CASCADE,
  CONSTRAINT "twitter-tweets-users-tweet-id-user-id" PRIMARY KEY("tweet-id", "user-id")
);

COMMIT;
