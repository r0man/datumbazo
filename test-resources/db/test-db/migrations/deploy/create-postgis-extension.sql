-- Deploy create-postgis-extension

BEGIN;

CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;

COMMIT;
