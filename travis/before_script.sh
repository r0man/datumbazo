#!/usr/bin/env bash

mysql -uroot -e "CREATE DATABASE datumbazo;"
mysql -uroot -e "GRANT ALL PRIVILEGES ON datumbazo.* TO tiger@localhost IDENTIFIED BY 'scotch';"

psql -c "CREATE DATABASE datumbazo;" -U postgres
psql -c "CREATE USER tiger SUPERUSER UNENCRYPTED PASSWORD 'scotch';" -U postgres
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-postgis-extension.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-citext-extension.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-continents-table.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-countries-table.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-twitter-schema.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-twitter-users-table.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-twitter-tweets-table.sql
psql -U postgres datumbazo < test-resources/db/test-db/migrations/deploy/create-twitter-tweets-users.sql

lein2 run "postgresql://tiger:scotch@localhost/datumbazo" test-resources/db/test-db/fixtures load
