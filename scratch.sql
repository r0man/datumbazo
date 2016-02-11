
select '[1,2,3]'::json -> 2;

select cast('[1,2,3]' as json) -> 2;

select '{"a":1,"b":2}'::json->'b';

select cast('{"a":1,"b":2}' as json) -> 'b';

\d

\d sqitch.*

select * from information_schema.tables;

select * from sqitch.projects;

select * from sqitch.tags;

select * from continents;

SELECT countries.id, countries.name, continents AS continent
  FROM countries
  JOIN continents
    ON continents.id = countries."continent-id";
