# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import sqlite3
from urllib.parse import urlencode

from yaal_provider import commit_then_close, fetch_dict_rows, rollback_then_close


class SQLiteContextManager:

    def __init__(self, options):
        self._options = options

    def get_context(self):
        return SQLiteDataProvider(self._options)


class SQLiteDataProvider:

    placeholder = "?"

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
        commit_then_close(con)

    def error(self):
        con = self._con
        self._con = None
        rollback_then_close(con)

    @staticmethod
    def get_value(parameter_type, value):
        if parameter_type == "blob":
            return sqlite3.Binary(value)
        return value

    def execute(self, sql, parameters):
        con = self._con
        cur = con.cursor()
        try:
            cur.execute(sql, parameters)
            rows = fetch_dict_rows(cur)
            return rows, cur.lastrowid
        finally:
            cur.close()
