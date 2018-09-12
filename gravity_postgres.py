import psycopg2 as pg
from psycopg2.extras import RealDictCursor


class PostgresContextManager:

    def __init__(self, options):
        self._options = options

    def get_context(self):
        return PostgresDataProvider(self._options)


class PostgresDataProvider:

    def __init__(self, options):
        self._db_name = options["db_name"]
        self._conn = None

    def begin(self):
        self._conn = pg.connect("dbname='" + self._db_name + "' user='postgres' password='admin'")

    def end(self):
        try:
            if self._conn:
                self._conn.commit()
                self._conn.close()
                self._conn = None
        except Exception as e:
            raise e

    def error(self):
        self._conn.rollback()
        self._conn.close()
        self._conn = None

    @staticmethod
    def get_value_converter(param_type, value):
        if param_type == "blob":
            return pg.Binary(value)
        return value

    def execute(self, query, input_shape, helper):
        con = self._conn
        content = helper.get_executable_content("%s", query)

        with con:
            cur = con.cursor(cursor_factory=RealDictCursor)
            args = helper.build_parameters(query, input_shape, self.get_value_converter)
            cur.execute(content, args)
            rows = cur.fetchall()

        return rows, cur.lastrowid
