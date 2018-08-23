import re
import sqlite3 as lite
import psycopg2
from psycopg2.extras import RealDictCursor

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class PostgresExecutionContext:

    def __init__(self, root_path, db_name):
        self._root_path = root_path
        self._conn = psycopg2.connect("dbname='" + db_name + "' user='postgres' password='admin'")
        pass

    def begin(self):
        pass

    def end(self):
        self._conn.commit()

    def error(self):
        self._conn.rollback()
    
    def get_value_converter(self, ptype, value):        
        if ptype == "blob":        
            return psycopg2.Binary(value)        
        return value
        
    def execute(self, node_query, input_shape):
        con = self._conn
        content = node_query.get_executable_content("%s")                            
        
        with con:
            cur = con.cursor(cursor_factory = RealDictCursor)
            args = node_query.build_parameter_values(input_shape, self.get_value_converter)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid       

class SQLiteExecutionContext:
    
    def __init__(self, root_path, db_name):        
        self._root_path = root_path
        db_path = root_path + "/db"
        self._con = lite.connect(db_path + "/" + db_name)

    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def get_value(self, ptype, value):        
        if ptype == "blob":        
            return lite.Binary(value)        
        return value

    def execute(self, node_query, input_shape):        
        con = self._con
        
        content = node_query.get_executable_content("?")                            
        con.row_factory = _dict_factory    
        with con:
            cur = con.cursor()
            args = node_query.build_parameter_values(input_shape, self.get_value)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid        