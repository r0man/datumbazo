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
