
CREATE TABLE continents (
  id serial PRIMARY KEY,
  name text UNIQUE NOT NULL,
  code varchar(2) UNIQUE NOT NULL,
  geometry geometry(MULTIPOLYGON, 4326),
  freebase_guid text UNIQUE,
  geonames_id integer UNIQUE,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX continents_name_fulltext_idx ON continents USING GIN(to_tsvector('english', name));
CREATE INDEX continents_geometry_idx ON continents USING GIST(geometry);

CREATE UNIQUE INDEX continents_name_idx ON continents (lower(name));
CREATE UNIQUE INDEX continents_code_idx ON continents (lower(code));
CREATE UNIQUE INDEX continents_geonames_id_idx ON continents (geonames_id);
CREATE UNIQUE INDEX continents_freebase_guid_idx ON continents (lower(freebase_guid));
