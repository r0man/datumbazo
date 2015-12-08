-- Deploy create-twitter-users-table

BEGIN;

CREATE TABLE twitter.users (
  id bigint PRIMARY KEY,
  "screen-name" text NOT NULL,
  name text NOT NULL,
  description text,
  "followers-count" integer NOT NULL DEFAULT 0,
  "friends-count" integer NOT NULL DEFAULT 0,
  "listed-count" integer NOT NULL DEFAULT 0,
  "retweet-count" integer NOT NULL DEFAULT 0,
  "statuses-count" integer NOT NULL DEFAULT 0,
  verified boolean NOT NULL DEFAULT false,
  "possibly-sensitive" boolean NOT NULL DEFAULT false,
  location text,
  "time-zone" text,
  lang varchar(2),
  url text,
  "profile-image-url" text,
  "default-profile-image" boolean NOT NULL DEFAULT true,
  "created-at" timestamp with time zone NOT NULL DEFAULT now(),
  "updated-at" timestamp with time zone NOT NULL DEFAULT now()
);

COMMIT;
