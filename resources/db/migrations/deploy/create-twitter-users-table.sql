-- Deploy create-twitter-users-table

BEGIN;

CREATE TABLE twitter.users (
  id integer PRIMARY KEY,
  screen_name text NOT NULL,
  name text NOT NULL,
  followers_count integer NOT NULL DEFAULT 0,
  friends_count integer NOT NULL DEFAULT 0,
  retweet_count integer NOT NULL DEFAULT 0,
  statuses_count integer NOT NULL DEFAULT 0,
  verified boolean NOT NULL DEFAULT false,
  possibly_sensitive boolean NOT NULL DEFAULT false,
  location text,
  time_zone text,
  lang varchar(2),
  url text,
  profile_image_url text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

COMMIT;
