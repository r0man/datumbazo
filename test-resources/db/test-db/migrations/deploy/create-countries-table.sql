-- Deploy create-countries-table

BEGIN;

CREATE TABLE countries (
  id serial PRIMARY KEY,
  "continent-id" integer REFERENCES continents (id),
  name citext UNIQUE NOT NULL,
  geometry geometry(MULTIPOLYGON, 4326),
  "freebase-guid" citext UNIQUE NOT NULL,
  "geonames-id" integer UNIQUE NOT NULL,
  "iso-3166-1-alpha-2" varchar(2) NOT NULL,
  "iso-3166-1-alpha-3" varchar(3) NOT NULL,
  "iso-3166-1-numeric" integer UNIQUE NOT NULL,
  "created-at" timestamp with time zone,
  "updated-at" timestamp with time zone
);

CREATE INDEX "countries-name-fulltext-idx" ON countries USING GIN(to_tsvector('english', name));

CREATE UNIQUE INDEX "countries-name-idx" ON countries (lower(name));

CREATE INDEX "countries-geometry-idx" ON countries USING GIST(geometry);

COMMIT;
