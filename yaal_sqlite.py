import sqlite3


class SQLiteContextManager:

    def __init__(self, options):
        self._options = options

    def get_context(self):
        return SQLiteDataProvider(self._options)


class SQLiteDataProvider:

    def __init__(self, options):
        self._database = options["database"]
        if self._database == "":
            self._database = ":memory:"
        self._con = None

    @staticmethod
    def _sqlite_dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def begin(self):
        self._con = sqlite3.connect(self._database)
        self._con.row_factory = self._sqlite_dict_factory

    def end(self):
        try:
            if self._con:
                self._con.commit()
                self._con.close()
        except Exception as e:
            raise e
        finally:
            self._con = None

    def error(self):
        if self._con:
            self._con.rollback()
            self._con.close()
            self._con = None

    @staticmethod
    def get_value(parameter_type, value):
        if parameter_type == "blob":
            return sqlite3.Binary(value)
        return value

    def execute(self, twig, input_shape, helper):
        con = self._con
        sql = helper.get_executable_content("?", twig, input_shape)
        with con:
            cur = con.cursor()
            args = helper.build_parameters(sql, input_shape, self.get_value)
            cur.execute(sql["content"], args)
            rows = cur.fetchall()

        return rows, cur.lastrowid
