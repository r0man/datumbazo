select "facebook-url" from users;



select '[1,2,3]'::json -> 2;

select cast('[1,2,3]' as json) -> 2;

select '{"a":1,"b":2}'::json->'b';

select cast('{"a":1,"b":2}' as json) -> 'b';

\d

\d sqitch.*

select * from information_schema.tables;

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
