#!/usr/bin/env bash

# set -e
set -x

PG_VER=11

# Remove MySQL

sudo systemctl stop mysql
sudo apt-get -y -qq --purge remove mysql-common mysql-server mysql-client
sudo rm /etc/mysql /var/lib/mysql /var/log/mysql -rf

# Install MariaDB

sudo apt-get install software-properties-common
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
sudo add-apt-repository 'deb [arch=amd64,arm64,i386,ppc64el] http://mirror.23media.de/mariadb/repo/10.1/ubuntu xenial main'
sudo apt-get update -qq

sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install mariadb-server mariadb-server-10.1
sudo systemctl restart mariadb

# Remove existing PostgreSQL

sudo systemctl stop postgresql
sudo apt-get -y -qq --purge remove postgresql libpq-dev libpq5 postgresql-client-common postgresql-common
sudo rm -rf /var/lib/postgresql

# Install PostgreSQL

sudo apt-get install curl ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update -qq

sudo apt-cache search postgis
sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install postgresql-${PG_VER} postgresql-server-dev-${PG_VER} postgresql-${PG_VER}-postgis-2.5 postgresql-${PG_VER}-postgis-scripts postgis
sudo find /etc/postgresql
echo "local all all trust" | sudo tee /etc/postgresql/${PG_VER}/main/pg_hba.conf
echo "host all all 127.0.0.1/32 trust" | sudo tee -a /etc/postgresql/${PG_VER}/main/pg_hba.conf
echo "host all all ::1/128 trust" | sudo tee -a /etc/postgresql/${PG_VER}/main/pg_hba.conf
sudo systemctl restart postgresql
