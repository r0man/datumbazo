-- Revert create-postgis-extension

BEGIN;

DROP EXTENSION postgis;

COMMIT;
