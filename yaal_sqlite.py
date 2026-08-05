import sqlite3
from urllib.parse import urlencode


class SQLiteContextManager:

    def __init__(self, options):
        self._options = options

    def get_context(self):
        return SQLiteDataProvider(self._options)


class SQLiteDataProvider:

    def __init__(self, options):
        self._options = options
        self._database = options.get("database") or ""
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
        query = self._options.get("query") or {}
        if query:
            if self._database == ":memory:":
                uri = "file::memory:?%s" % urlencode(query)
            else:
                uri = "file:%s?%s" % (self._database, urlencode(query))
            self._con = sqlite3.connect(uri, uri=True)
        else:
            self._con = sqlite3.connect(self._database)
        self._con.row_factory = self._sqlite_dict_factory

    def end(self):
        con = self._con
        self._con = None
        if not con:
            return
        try:
            con.commit()
        finally:
            con.close()

    def error(self):
        con = self._con
        self._con = None
        if not con:
            return
        try:
            con.rollback()
        finally:
            con.close()

    @staticmethod
    def get_value(parameter_type, value):
        if parameter_type == "blob":
            return sqlite3.Binary(value)
        return value

    def execute(self, twig, input_shape, helper):
        con = self._con
        sql = helper.get_executable_content("?", twig, input_shape)
        cur = con.cursor()
        try:
            args = helper.build_parameters(sql, input_shape, self.get_value)
            cur.execute(sql["content"], args)
            rows = cur.fetchall() if cur.description is not None else []
            return rows, cur.lastrowid
        finally:
            cur.close()
