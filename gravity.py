import os, argparse, copy, datetime, re
import json, yaml
from collections import defaultdict

import sqlite3 as lite, psycopg2 as pg
from psycopg2.extras import RealDictCursor

from shape import Shape

parameters_meta_rx = re.compile(r"--\((.*)\)--")
parameter_meta_rx = re.compile(r"\s*([A-Za-z0-9_.$-]+)(\s+(\w+))?\s*")
parameter_rx = re.compile(r"\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)
query_rx = re.compile(r"--query\(([a-zA-Z0-9.$_,]*?)\)\(([a-zA-Z0-9.$_]*?)\)--")

def _get_query_output(node_descriptor, execution_contexts, input_shape):
    input_type = node_descriptor["input_type"]
    actions = node_descriptor["actions"]
    execution_context = execution_contexts["db"]
    
    paramsstr = "$params"
    errorstr = "$error"
    breakstr = "$break"

    rs = []
    if actions is not None: 
        for action in actions:                            
            query_connection = action["connection"]
            if query_connection is not None:
                if query_connection in execution_contexts:
                    output, output_last_inserted_id = execution_contexts[query_connection].execute(action, input_shape, parameter_rx, _build_parameter_values)
                else:
                    raise Exception("connection string " + query_connection + " missing")
            else:
                output, output_last_inserted_id = execution_context.execute(action, input_shape, parameter_rx, _build_parameter_values)

            input_shape.set_prop("$params.$last_inserted_id", output_last_inserted_id)

            if len(output) >= 1:
                output0 = output[0]
                if errorstr in output0 and output0[errorstr] == 1:
                    for o in output:
                        del o[errorstr] 
                    raise Exception(output)
                elif paramsstr in output0 and output0[paramsstr] == 1:
                    for k, v in output0.items():
                        input_shape.set_prop("$params." + k, v)
                elif breakstr in output0 and output0[breakstr] == 1:
                    for o in output:
                        del o[breakstr] 
                    break
                else:
                    if input_type is not None:
                        if input_type == "array":
                            rs.extend(output)
                        elif input_type == "object":
                            rs = output
                    else:
                        rs = output
    return rs

def _execute_query(node_descriptor, execution_contexts, input_shape, parent_rows, parent_partition_by):
    input_type = node_descriptor["input_type"]
    output_partition_by = node_descriptor["partition_by"]

    use_parent_rows = node_descriptor["use_parent_rows"]     
    execution_context =  execution_contexts["db"]
    root = node_descriptor["root"]
    output = []
    try:
        if root:
            execution_context.begin()

        if input_type is not None and input_type == "array":
            length = int(input_shape.get_prop("$length"))
            for i in range(0, length):
                item_input_shape = input_shape.get_prop("@" + str(i))                    
                output.extend(_get_query_output(node_descriptor, execution_contexts, item_input_shape))
                
        elif input_type is None or input_type == "object":                
            output = _get_query_output(node_descriptor, execution_contexts, input_shape)
                
            if use_parent_rows == True and parent_partition_by is None:
                raise Exception("parent _partition_by is can't be empty when child wanted to use parent rows")
            
            if use_parent_rows == True:
                output = copy.deepcopy(parent_rows)

            childrens = node_descriptor["childrens"]
            if childrens:
                for child_descriptor in childrens:                    
                    sub_node_name = child_descriptor["name"]                    
                    sub_node_shape = None
                    if input_shape is not None:
                        sub_node_shape = input_shape.get_prop(sub_node_name)

                    sub_node_output = _execute_query(child_descriptor, execution_contexts, sub_node_shape, output, output_partition_by)                    
                                        
                    if not node_descriptor["actions"] and not output:
                        output.append({})
                        
                    if output_partition_by is None:
                        for row in output:
                            row[sub_node_name] = sub_node_output                        
                    else:
                        sub_node_groups = defaultdict(list)
                        for row in sub_node_output:
                            sub_node_groups[row[output_partition_by]].append(row)                            
                        
                        groups = defaultdict(list)
                        for row in output:
                            groups[row[output_partition_by]].append(row) 

                        _output = []
                        for idx,rows in groups.items():
                            row = rows[0]
                            partitionby = row[output_partition_by]
                            row[sub_node_name] = sub_node_groups[partitionby]
                            _output.append(row)
                        output = _output
            else:
                pass

    except Exception as e:
        execution_context.error()  
        raise e
    finally:
        if root:
            execution_context.end()
    
    return output

def _output_mapper(node_descriptor, result):
    mapped_result = []
    
    output_model = node_descriptor["output_model"]
    if output_model and "properties" in output_model:
        output_properties = output_model["properties"]
    else:
        output_properties = None
    
    output_type = node_descriptor["output_type"]

    for row in result:
        mapped_row = {}
        sub_mapped_nodes = {}
        childrens = node_descriptor["childrens"]
        if childrens:
            for child_descriptor in childrens:                    
                sub_node_name = child_descriptor["name"]
                
                if sub_node_name in row:
                    sub_mapped_nodes[sub_node_name] = _output_mapper(child_descriptor, row[sub_node_name])
        
        _typestr = "type"
        _mappedstr = "mapped"                                  
        if output_properties is not None:
            prop_count = 0
            for k, v in output_properties.items():
                if type(v) == dict and (_typestr in v or _mappedstr in v):
                    if _mappedstr in v:
                        _mapped = v[_mappedstr]
                        if _mapped in row:
                            mapped_row[k] = row[_mapped]
                            prop_count = prop_count + 1
                        else:
                            raise Exception(_mapped + " _mapped column missing from row")
                    else:
                        if _typestr in v:
                            _type = v[_typestr]       
                            if _type == "array" or _type == "object":
                                mapped_row[k] = sub_mapped_nodes[k]
            if prop_count == 0:
                mapped_row = row                                   
        else:
            mapped_row = row

        for k, v in sub_mapped_nodes.items():
            mapped_row[k] = v

        mapped_result.append(mapped_row)

    if output_type == "object":
        if len(mapped_result) > 0:
            mapped_result = mapped_result[0]
        else:
            mapped_result = {}
    return mapped_result

def _treemap(treemap, item):
    if item == "":
        return
    item = item
    dot = item.find(".")
    if  dot > -1:
        path = item[0:dot]
        remaining_path = item[dot + 1:]
        if path not in treemap:
            treemap[path] = {}
        _treemap(treemap[path], remaining_path)
    else:
        if item not in treemap:
            treemap[item] = {}

def _build_treemap(namelist):
    treemap = {}
    if namelist is not None:
        for item in namelist:
            _treemap(treemap, item)
    return treemap

def _build_descriptor_query(node_descriptor, content):
    if content is None or content == '':
        return

    lines = content.splitlines()

    if len(lines) == 0:
        return

    first_line = lines[0]
    parameters_first_line_m = parameters_meta_rx.match(first_line)

    meta = {}
    if parameters_first_line_m is not None:
        params_meta = parameters_first_line_m.groups(1)[0].split(",")
        for p in params_meta:
            parameter_meta_m = parameter_meta_rx.match(p)

            if parameter_meta_m is not None:
                gm = parameter_meta_m.groups(1)
                if len(gm) > 0:
                    parameter_name = gm[0].lower()
                if len(gm) > 1:
                    parameter_type = gm[2]                
                #prop_def = node_descriptor.get_input_propery_definition(parameter_name)                
                meta[parameter_name] = {
                    "name" : parameter_name,
                    "type" : parameter_type
                }
        node_descriptor["parameters"] = meta
    else:
        node_descriptor["parameters"] = None

    actions = []
    content = ""        
    connection = None

    for idx, line in enumerate(lines):
        if idx == 0 and parameters_first_line_m is not None:
            continue
        
        query_match = query_rx.match(line)
        if query_match is not None:                
            if content.lstrip().rstrip() is not "":                
                action = {
                    "content" : content,                    
                    "connection" : connection
                }
                _build_query_parameters(action, node_descriptor)
                actions.append(action)
            
            connection = query_match.groups(0)[1].lstrip().rstrip()            
            if connection == "":
                connection = None

            content = ""
        else:
            content = "\r\n".join([content, line])            
    
    if content.lstrip().rstrip() is not "":        
        action = {
            "content" : content,            
            "connection" : connection
        }        
        _build_query_parameters(action, node_descriptor)
        actions.append(action)

    node_descriptor["actions"] = actions

def _build_query_parameters(query, node_descriptor):
    content = query["content"]
    parameter_names = [x.lower() for x in parameter_rx.findall(content)]
    node_descriptor_parameters = node_descriptor["parameters"]

    params = []
    for p in parameter_names:
        node_query_parameter = None
        if node_descriptor_parameters is not None and p in node_descriptor_parameters:
            node_descriptor_parameter = node_descriptor_parameters[p]
            node_query_parameter = {
                "name": p,
                "type": node_descriptor_parameter["type"]
            }
            params.append(node_query_parameter)                            
        else:
            raise TypeError("type missing for {{" + p + "}} in the " + node_descriptor["method"] + ".sql")
    
    if len(params) != 0:
        query["parameters"] = params
    else:
        query["parameters"] = None

def _build_parameter_values(query ,input_shape, get_value_converter):
    values = []
    _cache = {}
    parameters = query["parameters"]
    if parameters is not None:
        for p in parameters:
            pname = p["name"]
            ptype = p["type"]

            if pname in _cache:
                pvalue = _cache[pname]
            else:
                pvalue = input_shape.get_prop(pname)
                _cache[pname] = pvalue
                
            try:
                if ptype == "integer":
                    pvalue = int(pvalue)
                elif ptype == "string":
                    pvalue = str(pvalue)
                else:
                    pvalue = get_value_converter(pvalue)
                values.append(pvalue)
            except:
                values.append(pvalue)
    
    return values

def _build_descriptor(node_descriptor, treemap, content_reader, input_model, output_model):
    _propertiesstr, _typestr, _partitionbystr, _parentrowsstr = "properties", "type", "partition_by", "parent_rows"

    path = node_descriptor["path"]
    method = node_descriptor["method"]

    content = content_reader.get_sql(method, path)
    sub_nodes_names = {}
    
    if input_model is None:
        input_model = {
            "type" : "object",
            _propertiesstr : {}
        }
    if _propertiesstr not in input_model:
        input_model[_propertiesstr] = {}

    node_descriptor["input_model"] = input_model
    node_descriptor["input_type"] = input_model[_typestr]    
    input_properties = input_model[_propertiesstr]    
    
    node_descriptor["output_model"] = output_model or { "type" : "array", "properties" : {} }
    _build_descriptor_query(node_descriptor, content)

    if "parameters" not in node_descriptor:
        node_descriptor["parameters"] = None

    if "actions" not in node_descriptor:
        node_descriptor["actions"] = None

    output_properties = None      
    if output_model is not None:
        if _typestr in output_model:
            node_descriptor["output_type"] = output_model[_typestr]
        else:
            node_descriptor["output_type"] = "array"

        if _propertiesstr in output_model:
            output_properties = output_model[_propertiesstr]            

        if _parentrowsstr in output_model:
            node_descriptor["use_parent_rows"] = output_model[_parentrowsstr]
        else:
            node_descriptor["use_parent_rows"] = None

        if _partitionbystr in output_model:
            node_descriptor["partition_by"] = output_model[_partitionbystr]
        else:
            node_descriptor["partition_by"] = None
                
        if output_properties is not None:
            for k in output_properties:
                v = output_properties[k]
                if type(v) == dict and _typestr in v:
                    _type = v[_typestr]
                    if _type == "object" or _type == "array":                      
                        sub_nodes_names[k] = {}
    else:
        node_descriptor["output_type"] = "array"        
        node_descriptor["use_parent_rows"] = False
        node_descriptor["partition_by"] = None

    for k in treemap:
        if k not in sub_nodes_names:
            sub_nodes_names[k] = treemap[k]

    childrens = []
    for child_method_name in sub_nodes_names:
        v = sub_nodes_names[child_method_name]
        mname = ".".join([method, child_method_name])
        child_descriptor = {
            "name" : child_method_name,
            "method" : mname,
            "path" : path,
            "root" : False
        }        

        sub_input_model = None
        sub_output_model = None
                    
        if child_method_name not in input_properties:
            input_properties[child_method_name] = {
                "type" : "object",
                "properties" : {}
            }
        sub_input_model = input_properties[child_method_name]
        
        if output_properties is not None:                
            if child_method_name in output_properties:
                sub_output_model = output_properties[child_method_name]

        _build_descriptor(child_descriptor, v, content_reader, sub_input_model, sub_output_model)
        childrens.append(child_descriptor)
    
    if len(childrens) != 0:
        node_descriptor["childrens"] = childrens
    else:
        node_descriptor["childrens"] = None

def _order_list_by_dots(names):
    if names is None:
        return []

    dots = [x.count(".") for x in names]
    ordered = []
    for x in range(0, len(dots)):
        if len(dots) == 0:
            break

        el = min(dots)
        while True:
            try:
                idx = dots.index(el)
                ordered.append(names[idx])
                del names[idx]
                del dots[idx]
            except:
                break
    return ordered

def get_result(node_descriptor, execution_contexts, input_shape):
    try:
        if "db" not in execution_contexts:
            raise Exception("default connection string name db is missing")
    
        rs = _execute_query(node_descriptor, execution_contexts, input_shape, [], None)           
        rs = _output_mapper(node_descriptor, rs)
        
        return rs
    except Exception as e:
        raise e            
        #return { "errors" : e.args[0] }

def _default_date_time_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def get_result_json(node_descriptor, execution_contexts, input_shape):
    return json.dumps(get_result(node_descriptor, execution_contexts, input_shape), default= _default_date_time_converter)

def create_input_shape(node_descriptor, request_body, params, query, path):
    params_shape = Shape({}, None, None, None, None, None)
    if params is not None:
        for k, v in params.items():
            params_shape.set_prop(k, v)
            
    query_shape = Shape(node_descriptor["input_query"], None, None, None, None, None)
    if query is not None:
        for k, v in query.items():
            query_shape.set_prop(k, v)        

    path_shape = Shape(node_descriptor["input_path"], None, None, None, None, None)
    if path is not None:
        for k, v in path.items():
            path_shape.set_prop(k, v)

    input_model = node_descriptor["input_model"]   
    return Shape(input_model, request_body, None, params_shape, query_shape, path_shape)

def create(method, path, content_reader):
    ordered_files = _order_list_by_dots(content_reader.list_sql(method, path))       
    if len(ordered_files) == 0: 
        return None # found zero sql files, then return no api available

    treemap = _build_treemap(ordered_files)        
    config = content_reader.get_config(method, path)
    
    input_model_str = "input.model"
    output_model_str = "output.model"
    input_body_str = "body"
    input_query_str = "query"
    input_path_str = "path"
    
    input_model = None
    output_model = None
    
    input_query, input_path =  None, None
    
    if config is not None:
        if input_model_str in config:
            input_config = config[input_model_str]
            if input_config is not None:
                if input_body_str in input_config:
                    input_model = input_config[input_body_str]
                if input_query_str in input_config:
                    input_query = {
                        "type" : "object",
                        "properties" : input_config[input_query_str]
                    }
                if input_path_str in input_config:
                    input_path = {
                        "type" : "object",
                        "properties" : input_config[input_path_str]
                    }
            
        if output_model_str in config:
            output_model = config[output_model_str]
    
    if not input_query:
        input_query = {
            "type" : "object",
            "properties" : {}
        }
    if not input_path:
        input_path = {
            "type" : "object",
            "properties" : {}
        }

    node_descriptor = {
        "name" : method,
        "method" : method,
        "path" : path,        
        "root": True,
        "input_query" : input_query,
        "input_path" : input_path
    }
    _build_descriptor(node_descriptor, treemap[method], content_reader, input_model, output_model)

    return node_descriptor

def _sqlite_dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SQLiteExecutionContext:
    
    def __init__(self, root_path, db_name):        
        self._root_path = root_path
        db_path = root_path + "/db"
        self._con = lite.connect(db_path + "/" + db_name)
        self._con.row_factory = _sqlite_dict_factory

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

    def execute(self, query, input_shape, parameter_rx, build_parameter_values):        
        con = self._con        
        content = parameter_rx.sub("?", query["content"])                                        
        with con:
            cur = con.cursor()
            args = build_parameter_values(query, input_shape, self.get_value)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid        

class FileContentReader:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_sql(self, method, path):
        file_path = os.path.join(*[self._root_path, path, method + ".sql"])
        return self._get(file_path)
    
    def get_config(self, method, path):
            
        input_path = os.path.join(*[self._root_path, path, method + ".input"])        
        input_config = self._get_config(input_path)

        output_path = os.path.join(*[self._root_path, path, method + ".output"])        
        output_config = self._get_config(output_path)

        if input_config is None and output_config is None:
            return None 

        return { "input.model" : input_config, "output.model" : output_config }

    def list_sql(self, method, path):
        try:     
            files = os.listdir(os.path.join(*[self._root_path, path]))
            ffiles = [f.replace(".sql", "") for f in files if f.startswith(method) and f.endswith(".sql")]        
        except:
            ffiles = None
        return ffiles
        
    def _get_config(self, filepath):
        yaml_path = filepath + ".yaml"
        if os.path.exists(yaml_path):
            config_str = self._get(yaml_path)
            if config_str is not None and config_str != '':
                return yaml.load(config_str)
        
        json_path = filepath + ".json"
        if os.path.exists(json_path):
            config_str = self._get(json_path)
            if config_str is not None and config_str != '':
                return json.loads(config_str)

    def _get(self, file_path):
        try:
            #print(file_path)
            with open(file_path, "r") as file:
                content = file.read()
        except:
            content = None
        return content

class Gravity:

    def __init__(self, root_path, content_reader):
        self._descriptors = {}        
        self._root_path = root_path
        if content_reader is None:
            self._content_reader = FileContentReader(self._root_path)
        else:
            self._content_reader = content_reader

    def create_execution_contexts(self):        
        execution_contexts = {
            "db" : PostgresExecutionContext(self._root_path, "dvdrental"),
            "sqlite3" : SQLiteExecutionContext(self._root_path, "sqlite3.db"),
            "app.db": SQLiteExecutionContext(self._root_path, "app.db")
        }
        return execution_contexts

    def create_descriptor(self, method, path, debug):
            
        k = os.path.join(*[path, method])        
        if debug == False and k in self._descriptors:
            return self._descriptors[k]            
        
        descriptor = create(method, path, self._content_reader)
        self._descriptors[k] = descriptor
        return descriptor

class PostgresExecutionContext:

    def __init__(self, root_path, db_name):
        self._root_path = root_path
        self._conn = pg.connect("dbname='" + db_name + "' user='postgres' password='admin'")
        pass

    def begin(self):
        pass

    def end(self):
        self._conn.commit()

    def error(self):
        self._conn.rollback()
    
    def get_value_converter(self, ptype, value):        
        if ptype == "blob":        
            return pg.Binary(value)        
        return value
        
    def execute(self, query, input_shape, parameter_rx, build_parameter_values):
        con = self._conn
        content = parameter_rx.sub("%s", query["content"])
        
        with con:
            cur = con.cursor(cursor_factory = RealDictCursor)
            args = build_parameter_values(query, input_shape, self.get_value_converter)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid