import psycopg2 as pg
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from yaal_provider import commit_then_close, rollback_then_close


_CONNECT_QUERY_KEYS = (
    "sslmode",
    "sslcert",
    "sslkey",
    "sslrootcert",
    "connect_timeout",
    "application_name",
    "options",
)


class PostgresContextManager:

    def __init__(self, options):
        port = options.get("port")
        kwargs = {
            "user": options["username"],
            "password": options["password"],
            "host": options["host"],
            "port": int(port) if port else 5432,
            "database": options["database"],
        }
        query = options.get("query") or {}
        for key in _CONNECT_QUERY_KEYS:
            if key in query:
                kwargs[key] = query[key]
        self._pool = pool.SimpleConnectionPool(1, 20, **kwargs)

    def get_context(self):
        return PostgresDataProvider(self._pool)


class PostgresDataProvider:

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    def begin(self):
        self._conn = self._pool.getconn()

    def end(self):
        conn = self._conn
        self._conn = None
        commit_then_close(conn, release=self._pool.putconn)

    def error(self):
        conn = self._conn
        self._conn = None
        rollback_then_close(conn, release=self._pool.putconn)

    @staticmethod
    def get_value_converter(param_type, value):
        if param_type == "blob":
            return pg.Binary(value)
        return value

    @staticmethod
    def _last_inserted_id(cur, rows):
        if not (cur.statusmessage and cur.statusmessage.startswith("INSERT")):
            return None
        if not rows or len(rows) != 1:
            return None
        row = rows[0]
        if "id" in row:
            return row["id"]
        if len(row) == 1:
            return next(iter(row.values()))
        return None

    def execute(self, twig, input_shape, helper):
        con = self._conn
        sql = helper.get_executable_content("%s", twig, input_shape)
        cur = con.cursor(cursor_factory=RealDictCursor)
        try:
            args = helper.build_parameters(sql, input_shape, self.get_value_converter)
            cur.execute(sql["content"], args)
            if cur.description is not None:
                rows = [dict(row) for row in cur.fetchall()]
            else:
                rows = []
            return rows, self._last_inserted_id(cur, rows)
        finally:
            cur.close()
