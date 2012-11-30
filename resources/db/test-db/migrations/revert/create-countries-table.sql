-- Revert create-countries-table

BEGIN;

DROP TABLE countries;

COMMIT;
