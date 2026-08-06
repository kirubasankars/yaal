-- Idempotent seed for `make experiment-clickhouse` / experiment-clickhouse-reset.
-- Schema matches docker/clickhouse/init.sql; truncates then reloads fixture rows.

CREATE DATABASE IF NOT EXISTS yaal;

CREATE TABLE IF NOT EXISTS yaal.users (
    user_id   Int32,
    user_name String,
    active    UInt8
) ENGINE = MergeTree
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS yaal.roles (
    role_id   Int32,
    role_name String,
    active    UInt8
) ENGINE = MergeTree
ORDER BY role_id;

CREATE TABLE IF NOT EXISTS yaal.user_roles (
    user_id Int32,
    role_id Int32
) ENGINE = MergeTree
ORDER BY (user_id, role_id);

TRUNCATE TABLE IF EXISTS yaal.user_roles;
TRUNCATE TABLE IF EXISTS yaal.users;
TRUNCATE TABLE IF EXISTS yaal.roles;

INSERT INTO yaal.users (user_id, user_name, active) VALUES
    (1, 'admin', 1),
    (2, 'guest', 1);

INSERT INTO yaal.roles (role_id, role_name, active) VALUES
    (1, 'Administrator', 1),
    (2, 'User', 1);

INSERT INTO yaal.user_roles (user_id, role_id) VALUES
    (1, 1),
    (1, 2),
    (2, 2);
