#!/bin/sh
# Runs once, on first MySQL start: a separate database for integration tests.
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<SQL
CREATE DATABASE IF NOT EXISTS taskqueue_test;
GRANT ALL PRIVILEGES ON taskqueue_test.* TO '$MYSQL_USER'@'%';
SQL
