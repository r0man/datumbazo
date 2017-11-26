#!/usr/bin/env bash
set -x
PG_VER=10
sudo service postgresql stop
sudo apt-get -y -qq --purge remove postgresql libpq-dev libpq5 postgresql-client-common postgresql-common
sudo rm -rf /var/lib/postgresql
sudo apt-get update -qq
sudo apt-cache search postgis
sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install postgresql-${PG_VER} postgresql-contrib-${PG_VER} postgresql-server-dev-${PG_VER} postgresql-${PG_VER}-postgis-2.2 postgresql-${PG_VER}-postgis-scripts postgis
echo "local all all trust" | sudo tee /etc/postgresql/${PG_VER}/main/pg_hba.conf
echo "host all all 127.0.0.1/32 trust" | sudo tee -a /etc/postgresql/${PG_VER}/main/pg_hba.conf
echo "host all all ::1/128 trust" | sudo tee -a /etc/postgresql/${PG_VER}/main/pg_hba.conf
sudo service postgresql restart
