import re
import sqlite3 as lite

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SQLiteExecutionContext:

    def __init__(self):
        self._parameter_rx = re.compile("\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)

    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_executor, input):        
        node_descriptor = node_executor.get_node_descritor()
        content = node_descriptor.get_content()
        if content is None:
            return []
        content = self._parameter_rx.sub("?", content)                    
        con = lite.connect(":memory:")
        con.row_factory = _dict_factory        
        
        with con:
            cur = con.cursor()            
            args = []
            parameters = node_descriptor.get_parameters()
            if parameters is not None:
                for p in parameters:
                    args.append(input.get_prop(p.get_name()))
            cur.execute(content, args)
            rows = cur.fetchall()

        return rows        
