-- Deploy create-twitter-tweets-table

BEGIN;

CREATE TABLE twitter.tweets (
  id integer PRIMARY KEY,
  user_id integer REFERENCES twitter.users(id) ON DELETE CASCADE,
  retweeted boolean NOT NULL DEFAULT false,
  text text NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

COMMIT;
