import psycopg2 as pg
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


class PostgresContextManager:

    def __init__(self, options):
        self._pool = pool.SimpleConnectionPool(1, 20, user=options["username"],
                                               password=options["password"],
                                               host=options["host"],
                                               port=options["port"],
                                               database=options["database"])

    def get_context(self):
        return PostgresDataProvider(self._pool)


class PostgresDataProvider:

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    def begin(self):
        self._conn = self._pool.getconn()

    def end(self):
        try:
            if self._conn:
                self._conn.commit()
                self._pool.putconn(self._conn)
            else:
                self._pool.putconn(self._conn, close=True)
        except Exception as e:
            self._pool.putconn(self._conn, close=True)
            raise e

    def error(self):
        if self._conn:
            self._conn.rollback()
            self._conn.close()
            self._pool.putconn(self._conn, close=True)
            self._conn = None

    @staticmethod
    def get_value_converter(param_type, value):
        if param_type == "blob":
            return pg.Binary(value)
        return value

    def execute(self, twig, input_shape, helper):
        con = self._conn
        sql = helper.get_executable_content("%s", twig, input_shape)

        with con:
            cur = con.cursor(cursor_factory=RealDictCursor)
            args = helper.build_parameters(sql, input_shape, self.get_value_converter)
            print(sql["content"])
            cur.execute(sql["content"], args)
            rows = cur.fetchall()

        return rows, cur.lastrowid
