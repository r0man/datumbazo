
CREATE TABLE countries (
  id serial PRIMARY KEY,
  continent_id integer REFERENCES continents (id),
  name text UNIQUE NOT NULL,
  geometry geometry(MULTIPOLYGON, 4326),
  freebase_guid text UNIQUE NOT NULL,
  geonames_id integer UNIQUE NOT NULL,
  iso_3166_1_alpha_2 varchar(2) NOT NULL,
  iso_3166_1_alpha_3 varchar(3) NOT NULL,
  iso_3166_1_numeric integer UNIQUE NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);

CREATE INDEX countries_name_fulltext_idx ON countries USING GIN(to_tsvector('english', name));

CREATE UNIQUE INDEX countries_name_idx ON countries (lower(name));

CREATE INDEX countries_geometry_idx ON countries USING GIST(geometry);
