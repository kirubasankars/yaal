import psycopg2
from psycopg2.extras import RealDictCursor

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
