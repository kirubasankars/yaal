import mysql.connector
import mysql.connector.pooling


class MySQLContextManager:

    def __init__(self, options):
        db_config = {
            "database": options["database"],
            "user": options["username"],
            "password": options["password"],
            "host": options["host"],
            "port": options["port"]
        }
        self._pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=3, **db_config)

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
                self._conn.close()
            else:
                pass
        except Exception as e:
            #self._conn.close()
            raise e

    def error(self):
        if self._conn:
            self._conn.rollback()
            self._conn.close()
            self._conn = None

    @staticmethod
    def get_value_converter(param_type, value):
        return value

    def execute(self, twig, input_shape, helper):
        con = self._conn
        sql = helper.get_executable_content("%s", twig, input_shape)

        cur = con.cursor(dictionary=True)
        args = helper.build_parameters(sql, input_shape, self.get_value_converter)
        print(sql["content"])
        cur.execute(sql["content"], args)
        rows = cur.fetchall()

        return rows, cur.lastrowid
