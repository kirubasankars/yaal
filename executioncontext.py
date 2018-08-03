import re
import sqlite3 as lite

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SQLiteExecutionContext:
        
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_executor, input_shape):        
        node_descriptor = node_executor.get_node_descritor()
        
        content = node_descriptor.get_content()
        if content is None:
            return []
        
        content = node_descriptor.get_executable_content("?")                    
        
        con = lite.connect("chinook.db")
        con.row_factory = _dict_factory    
        with con:
            cur = con.cursor()
            args = node_descriptor.build_parameter_values(input_shape)
            cur.execute(content, args)
            rows = cur.fetchall()

        return rows        
