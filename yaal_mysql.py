# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import mysql.connector
import mysql.connector.pooling

from yaal_provider import (
    commit_then_close,
    fetch_dict_rows,
    parse_pool_int,
    rollback_then_close,
)


_CONNECT_QUERY_KEYS = (
    "charset",
    "collation",
    "ssl_ca",
    "ssl_cert",
    "ssl_key",
    "connection_timeout",
    "use_pure",
    "autocommit",
)


def _mysql_pool_release(conn, *, close):
    """Return a pooled connection via close(); discard on error when possible."""
    if conn is None:
        return
    if close:
        # Best-effort invalidate before returning / closing a bad connection.
        try:
            conn.unread_result = False
        except Exception:
            pass
        try:
            if hasattr(conn, "is_connected") and conn.is_connected():
                try:
                    conn.rollback()
                except Exception:
                    pass
        except Exception:
            pass
    try:
        # For PooledMySQLConnection, close() returns the connection to the pool.
        conn.close()
    except Exception:
        pass


class MySQLContextManager:

    def __init__(self, options):
        port = options.get("port")
        db_config = {
            "database": options["database"],
            "user": options["username"],
            "password": options["password"],
            "host": options["host"],
            "port": int(port) if port else 3306,
        }
        query = options.get("query") or {}
        for key in _CONNECT_QUERY_KEYS:
            if key in query:
                value = query[key]
                if key in ("connection_timeout",):
                    value = int(value)
                elif key in ("use_pure", "autocommit"):
                    value = str(value).lower() in ("1", "true", "yes")
                db_config[key] = value
        pool_size = parse_pool_int(query, "pool_size", 10)
        pool_name = "yaal-%s" % id(self)
        self._pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name, pool_size=pool_size, **db_config
        )

    def get_context(self):
        return MySQLDataProvider(self._pool)


class MySQLDataProvider:

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    def begin(self):
        self._conn = self._pool.get_connection()

    def end(self):
        conn = self._conn
        self._conn = None
        commit_then_close(conn, release=_mysql_pool_release)

    def error(self):
        conn = self._conn
        self._conn = None
        rollback_then_close(conn, release=_mysql_pool_release)

    @staticmethod
    def get_value_converter(param_type, value):
        return value

    def execute(self, twig, input_shape, helper):
        con = self._conn
        sql = helper.get_executable_content("%s", twig, input_shape)
        cur = con.cursor(dictionary=True)
        try:
            args = helper.build_parameters(sql, input_shape, self.get_value_converter)
            cur.execute(sql["content"], args)
            if cur.with_rows:
                rows = fetch_dict_rows(cur)
            else:
                rows = []
            return rows, cur.lastrowid
        finally:
            cur.close()
