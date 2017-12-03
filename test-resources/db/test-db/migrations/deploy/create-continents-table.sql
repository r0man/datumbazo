-- Deploy create-continents-table

BEGIN;

CREATE TABLE continents (
  id serial PRIMARY KEY,
  name citext UNIQUE NOT NULL,
  code varchar(2) UNIQUE NOT NULL,
  geometry geography(MULTIPOLYGON, 4326),
  "geonames-id" integer UNIQUE,
  "created-at" timestamp with time zone NOT NULL DEFAULT now(),
  "updated-at" timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX "continents-name-fulltext-idx" ON continents USING GIN(to_tsvector('english', name));
CREATE INDEX "continents-geometry-idx" ON continents USING GIST(geometry);

CREATE UNIQUE INDEX "continents-code-idx" ON continents (lower(code));
CREATE UNIQUE INDEX "continents-geonames-id-idx" ON continents ("geonames-id");

COMMIT;
