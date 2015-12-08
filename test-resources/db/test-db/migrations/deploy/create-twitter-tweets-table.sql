-- Deploy create-twitter-tweets-table

BEGIN;

CREATE TABLE twitter.tweets (
  id bigint PRIMARY KEY,
  "user-id" bigint REFERENCES twitter.users(id) ON DELETE CASCADE,
  retweeted boolean NOT NULL DEFAULT false,
  text text NOT NULL,
  "created-at" timestamp with time zone NOT NULL DEFAULT now(),
  "updated-at" timestamp with time zone NOT NULL DEFAULT now()
);

COMMIT;
