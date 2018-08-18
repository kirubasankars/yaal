import re

import psycopg2
from psycopg2.extras import RealDictCursor

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
