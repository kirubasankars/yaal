import mysql.connector
import mysql.connector.pooling


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
        pool_name = "yaal-%s" % id(self)
        self._pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name, pool_size=3, **db_config
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
        if not conn:
            return
        try:
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            raise
        conn.close()

    def error(self):
        conn = self._conn
        self._conn = None
        if not conn:
            return
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

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
                rows = cur.fetchall()
            else:
                rows = []
            return rows, cur.lastrowid
        finally:
            cur.close()
