-- Revert create-postgis-extension

BEGIN;

DROP EXTENSION postgis_raster;
DROP EXTENSION postgis;

COMMIT;
