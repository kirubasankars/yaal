import re
import sqlite3 as lite

import psycopg2
from psycopg2.extras import RealDictCursor

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SQLiteExecutionContext:
    
    def __init__(self, gravity_configuration, db_name):        
        self._gravity_configuration = gravity_configuration

        db_path = gravity_configuration.get_root_path() + "/db"
        self._con = lite.connect(db_path + "/" + db_name)

    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_query, input_shape):        
        con = self._con
        
        content = node_query.get_executable_content("?")                            
        con.row_factory = _dict_factory    
        with con:
            cur = con.cursor()
            args = node_query.build_parameter_values(input_shape)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid        


class PostgresExecutionContext:

    def __init__(self, gravity_configuration, db_name):
        self._gravity_configuration = gravity_configuration
        self._conn = psycopg2.connect("dbname='" + db_name + "' user='postgres' password='admin'")
        pass

    def begin(self):
        pass

    def end(self):
        pass

    def error(self):
        pass
    
    def execute(self, node_query, input_shape):
        con = self._conn
        content = node_query.get_executable_content("%s")                            
        
        with con:
            cur = con.cursor(cursor_factory = RealDictCursor)
            args = node_query.build_parameter_values(input_shape)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid       
