#!/usr/bin/env bash
dropdb database_test
./bin/createdb database_test
lein test
