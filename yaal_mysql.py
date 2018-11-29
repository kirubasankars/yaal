import mysql.connector


class MySQLContextManager:

    def __init__(self, options):
        self._pool = mysql.connector.connect(pool_name="mypool", pool_size=3, **options)

    def get_context(self):
        return MySQLDataProvider(self._pool)


class MySQLDataProvider:

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    def begin(self):
        self._conn = self._pool.get_connection()

    def end(self):
        try:
            if self._conn:
                self._conn.commit()
                self._pool.add_connection(self._conn)
            else:
                self._pool.add_connection(self._conn)
        except Exception as e:
            self._conn.close()
            raise e

    def error(self):
        if self._conn:
            self._conn.rollback()
            self._conn.close()
            self._conn = None

    @staticmethod
    def get_value_converter(param_type, value):

        return value

    def execute(self, query, input_shape, helper):
        con = self._conn
        sql = helper.get_executable_content("%s", query, input_shape)

        with con:
            cur = con.cursor(dictionary=True)
            args = helper.build_parameters(sql, input_shape, self.get_value_converter)
            print(sql["content"])
            cur.execute(sql["content"], args)
            rows = cur.fetchall()

        return rows, cur.lastrowid
