
create schema "my-schema";

select 1, 2, 3;

select 1 2 3;


create type "my-schema"."mood" as enum ('sad', 'ok', 'happy');

create type mood as enum ('sad', 'ok', 'happy');

drop type mood;

drop type "my-schema"."mood";

drop type "my-schema"."mood", "mood";

CREATE TABLE person (
  name TEXT,
  mood "my-schema"."mood"
);

DROP TABLE person;


SELECT (ARRAY[1,2,3,4,5])[2:0];

select "facebook-url" from users;



select '[1,2,3]'::json -> 2;

select cast('[1,2,3]' as json) -> 2;

select '{"a":1,"b":2}'::json->'b';

select cast('{"a":1,"b":2}' as json) -> 'b';

\d

\d sqitch.*

select * from information_schema.tables;

\d information_schema.columns;

select table_catalog, table_schema, column_name, data_type, udt_name, character_maximum_length from information_schema.columns where table_name = 'continents';

select * from information_schema.information_schema_catalog_name;

select * from sqitch.projects;

select * from sqitch.tags;

DROP TABLE IF EXISTS distributors;

CREATE TABLE distributors (
  did integer PRIMARY KEY,
  dname text,
  zipcode text
);

INSERT INTO "distributors" ("did", "dname")
  VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')
  ON CONFLICT ("did") DO UPDATE SET "dname" = EXCLUDED."dname";

INSERT INTO distributors AS d (did, dname) VALUES (8, 'Anvil Distribution')
    ON CONFLICT (did) DO UPDATE
    SET dname = EXCLUDED.dname || ' (formerly ' || d.dname || ')'
    WHERE d.zipcode <> '21201';

-- Materialize.io

CREATE MATERIALIZED VIEW pseudo_source (key, value) AS
    VALUES ('a', 1), ('a', 2), ('a', 3), ('a', 4),
    ('b', 5), ('c', 6), ('c', 7);

SELECT * FROM pseudo_source;

SELECT key, sum(value) FROM pseudo_source GROUP BY key;

CREATE MATERIALIZED VIEW key_sums AS
  SELECT key, sum(value) FROM pseudo_source GROUP BY key;

SELECT sum(sum) FROM key_sums;

CREATE MATERIALIZED VIEW lhs (key, value) AS
  VALUES ('x', 'a'), ('y', 'b'), ('z', 'c');

SELECT lhs.key, sum(rhs.value)
  FROM lhs
         JOIN pseudo_source AS rhs
             ON lhs.value = rhs.key
 GROUP BY lhs.key;

CREATE SOURCE wikirecent
  FROM FILE '/home/roman/workspace/datumbazo/wikirecent' WITH (tail = true)
  FORMAT REGEX '^data: (?P<data>.*)';

SHOW COLUMNS FROM wikirecent;

CREATE MATERIALIZED VIEW recentchanges AS
  SELECT
  val->>'$schema' AS r_schema,
  (val->'bot')::bool AS bot,
  val->>'comment' AS comment,
  (val->'id')::float::int AS id,
  (val->'length'->'new')::float::int AS length_new,
  (val->'length'->'old')::float::int AS length_old,
  val->'meta'->>'uri' AS meta_uri,
  val->'meta'->>'id' as meta_id,
  (val->'minor')::bool AS minor,
  (val->'namespace')::float AS namespace,
  val->>'parsedcomment' AS parsedcomment,
  (val->'revision'->'new')::float::int AS revision_new,
  (val->'revision'->'old')::float::int AS revision_old,
  val->>'server_name' AS server_name,
  (val->'server_script_path')::text AS server_script_path,
  val->>'server_url' AS server_url,
  (val->'timestamp')::float AS r_ts,
  val->>'title' AS title,
  val->>'type' AS type,
  val->>'user' AS user,
  val->>'wiki' AS wiki
  FROM (SELECT data::jsonb AS val FROM wikirecent);

CREATE MATERIALIZED VIEW counter AS
  SELECT COUNT(*) FROM recentchanges;

CREATE MATERIALIZED VIEW useredits AS
  SELECT user, count(*) FROM recentchanges GROUP BY user;

SELECT * FROM useredits ORDER BY count DESC;

CREATE MATERIALIZED VIEW top10 AS
  SELECT * FROM useredits ORDER BY count DESC LIMIT 10;

SELECT * FROM top10 ORDER BY count DESC;
