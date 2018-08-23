import re
import sqlite3 as lite

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

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