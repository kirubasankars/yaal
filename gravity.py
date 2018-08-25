import os, argparse, copy, datetime, re, json, yaml
from collections import defaultdict
from jsonschema import validate, FormatChecker, Draft4Validator

import sqlite3

import psycopg2 as pg
from psycopg2.extras import RealDictCursor

parameters_meta_rx = re.compile(r"--\((.*)\)--")
parameter_meta_rx = re.compile(r"\s*([A-Za-z0-9_.$-]+)(\s+(\w+))?\s*")
parameter_rx = re.compile(r"\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)
query_rx = re.compile(r"--query\(([a-zA-Z0-9.$_]*?)\)--")

def _get_query_output(node_descriptor, execution_contexts, input_shape, execution_context_helper):
    input_type = node_descriptor["input_type"]
    actions = node_descriptor["actions"]
    execution_context = execution_contexts["db"]
    
    paramsstr = "$params"
    errorstr = "$error"
    breakstr = "$break"

    rs = []
    if actions is not None: 
        for action in actions:
            if "connection" in action:
                query_connection = action["connection"]        
                if query_connection in execution_contexts:
                    output, output_last_inserted_id = execution_contexts[query_connection].execute(action, input_shape, execution_context_helper)
                else:
                    raise Exception("connection string " + query_connection + " missing")
            else:
                output, output_last_inserted_id = execution_context.execute(action, input_shape, execution_context_helper)

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

def _execute_query(node_descriptor, execution_contexts, input_shape, parent_rows, parent_partition_by, execution_context_helper):
    input_type = node_descriptor["input_type"]
    output_partition_by = node_descriptor["partition_by"]

    use_parent_rows = node_descriptor["use_parent_rows"]     
    execution_context =  execution_contexts["db"]
    
    if "root" in node_descriptor:
        root = True
    else:
        root = False
    
    output = []

    try:
        if root:
            execution_context.begin()

        if input_type is not None and input_type == "array":
            length = int(input_shape.get_prop("$length"))
            for i in range(0, length):
                item_input_shape = input_shape.get_prop("@" + str(i))                    
                output.extend(_get_query_output(node_descriptor, execution_contexts, item_input_shape, execution_context_helper))
                
        elif input_type is None or input_type == "object":                
            output = _get_query_output(node_descriptor, execution_contexts, input_shape, execution_context_helper)
                
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

                    sub_node_output = _execute_query(child_descriptor, execution_contexts, sub_node_shape, output, output_partition_by, execution_context_helper)                    
                                        
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

def _output_mapper(outputtype, output_modal, childrens, result):
    mapped_result = []
    
    output_model = output_modal
    if output_model and "properties" in output_model:
        output_properties = output_model["properties"]
    else:
        output_properties = None
    
    output_type = outputtype

    for row in result:
        mapped_obj = {}
        mapped_tree = {}
        childrens = childrens
        if childrens:
            for child_descriptor in childrens:                    
                
                child_descriptor_name = child_descriptor["name"]
                child_descriptor_output_type = child_descriptor["output_type"]

                if output_properties and child_descriptor_name in output_properties: 
                    child_descriptor_output_model = output_properties[child_descriptor_name]
                else:
                    child_descriptor_output_model = None

                child_descriptor_childrens = child_descriptor["childrens"]

                if child_descriptor_name in row:
                    mapped_tree[child_descriptor_name] = _output_mapper(child_descriptor_output_type, child_descriptor_output_model, child_descriptor_childrens, row[child_descriptor_name])
        
        _typestr = "type"
        _mappedstr = "mapped"                                  
        if output_properties is not None:
            prop_count = 0
            for k, v in output_properties.items():
                if type(v) == dict and (_typestr in v or _mappedstr in v):
                    if _mappedstr in v:
                        _mapped = v[_mappedstr]
                        if _mapped in row:
                            mapped_obj[k] = row[_mapped]
                            prop_count = prop_count + 1
                        else:
                            raise Exception(_mapped + " _mapped column missing from row")
                    else:
                        if _typestr in v:
                            _type = v[_typestr]       
                            if _type == "array" or _type == "object":
                                mapped_obj[k] = mapped_tree[k]
            if prop_count == 0:
                mapped_obj = row                                   
        else:
            mapped_obj = row

        for k, v in mapped_tree.items():
            mapped_obj[k] = v

        mapped_result.append(mapped_obj)

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
                    "content" : content
                }
                if connection:
                    action["connection"] = connection

                _build_query_parameters(action, node_descriptor)
                actions.append(action)
            
            connection = query_match.groups(0)[0].lstrip().rstrip()            
            if connection == "":
                connection = None

            content = ""
        else:
            content = "\r\n".join([content, line])            
    
    if content.lstrip().rstrip() is not "":        
        action = {
            "content" : content
        }
        if connection:
            action["connection"] = connection
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

def _build_descriptor(node_descriptor, root, treemap, content_reader, input_model, output_model):
    _propertiesstr, _typestr, _partitionbystr, _parentrowsstr = "properties", "type", "partition_by", "parent_rows"

    path = node_descriptor["path"]
    method = node_descriptor["method"]    
    content = content_reader.get_sql(method, path)
    node_tree = {}
    
    if input_model is None:
        input_model = {
            "type" : "object",
            _propertiesstr : {}
        }
    if _propertiesstr not in input_model:
        input_model[_propertiesstr] = {}

    if root:
        node_descriptor["input_model"] = input_model
        
    node_descriptor["input_type"] = input_model[_typestr]
    input_properties = input_model[_propertiesstr]    
    
    if root:
        node_descriptor["output_model"] = output_model or { "type" : "array", "properties" : {} }
    
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
                        node_tree[k] = {}
    else:
        node_descriptor["output_type"] = "array"        
        node_descriptor["use_parent_rows"] = False
        node_descriptor["partition_by"] = None

    _build_descriptor_query(node_descriptor, content)

    if "parameters" not in node_descriptor:
        node_descriptor["parameters"] = None

    if "actions" not in node_descriptor:
        node_descriptor["actions"] = None

    for k in treemap:
        if k not in node_tree:
            node_tree[k] = treemap[k]

    childrens = []
    for child_method_name in node_tree:
        sub_node_tree = node_tree[child_method_name]
        mname = ".".join([method, child_method_name])
        child_descriptor = {
            "name" : child_method_name,
            "method" : mname,
            "path" : path
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

        _build_descriptor(child_descriptor, False, sub_node_tree, content_reader, sub_input_model, sub_output_model)
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
    
    errors = input_shape.validate()
    if errors:
        return { "errors" : errors }

    try:        
        if "db" not in execution_contexts:
            raise Exception("default connection string name db is missing")
        execution_context_helper = ExecutionContextHelper(parameter_rx)
        rs = _execute_query(node_descriptor, execution_contexts, input_shape, [], None, execution_context_helper)
        rs = _output_mapper(node_descriptor["output_type"], node_descriptor["output_model"], node_descriptor["childrens"], rs)
        
        return rs
    except Exception as e:        
        return { "errors" : e.args[0] }

def _default_date_time_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def get_result_json(node_descriptor, execution_contexts, input_shape):    
    return json.dumps(get_result(node_descriptor, execution_contexts, input_shape), default= _default_date_time_converter)

def create_input_shape(node_descriptor, request_body, params, query, path):    
    
    input_query = node_descriptor["input_query"]    
    input_path = node_descriptor["input_path"]  
    input_model = node_descriptor["input_model"]

    params_shape = Shape({}, None, None, None)    
    if params is not None:
        for k, v in params.items():
            params_shape.set_prop(k, v)
    
    
    query_shape = Shape(input_query, None, None, None)    
    query_shape._validator = node_descriptor["input_query_schema_validator"]
    if query is not None:
        for k, v in query.items():
            query_shape.set_prop(k, v)        

    
    path_shape = Shape(input_path, None, None, None)
    path_shape._validator = node_descriptor["input_path_schema_validator"]
    if path is not None:
        for k, v in path.items():
            path_shape.set_prop(k, v)

    extras = {
        "$params" : params_shape, 
        "$query" : query_shape,
        "$path" :path_shape
    }

    input_model_shape = Shape(input_model, request_body, None, extras)
    input_model_shape._validator = node_descriptor["input_model_schema_validator"]
    return input_model_shape

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
        
    if input_query:
        node_descriptor["input_query_schema_validator"] = Draft4Validator(schema = input_query, format_checker = FormatChecker())
    else:
        node_descriptor["input_query_schema_validator"] = None

    if input_path:
        node_descriptor["input_path_schema_validator"] = Draft4Validator(schema = input_path, format_checker = FormatChecker())    
    else:
        node_descriptor["input_path_schema_validator"] = None
    
    if input_model:
        node_descriptor["input_model_schema_validator"] = Draft4Validator(schema = input_model, format_checker = FormatChecker())
    else:
        node_descriptor["input_model_schema_validator"] = None

    _build_descriptor(node_descriptor, True, treemap[method], content_reader, input_model, output_model)

    return node_descriptor

class Shape:
    
    def __init__(self, input_model, data, parent_shape, extras):        
        self._array = False
        self._object = False

        self._data = data or {}
        self._parent = parent_shape        
        self._input_model = input_model
        self._input_properties = None
        input_properties = None
        self._index = 0
        self._validator = None
        
        self._extras = extras        
        
        if data is not None and ("$parent" in data or "$length" in data):
            raise Exception("$parent or $length is reversed keywords. You can't use them.")

        if input_model is None:
            return
        
        _propertiesstr = "properties"
        if _propertiesstr in input_model:
            input_properties = input_model[_propertiesstr]
            self._input_properties = input_properties


        _typestr = "type"
        if _typestr in input_model:
            _type = input_model[_typestr]
            if _type == "array":
                self._array = True                
                if data is not None:
                    if type(data) != list:
                        raise TypeError("input expected as array. object is given.")
            else:
                self._object = True
                if data is not None:
                    if type(data) != dict:
                        raise TypeError("input expected as object. array is given.")
        
        if self._array:
            shapes = []            
            input_model[_typestr] = "object"
            idx = 0                
            for item in self._data:
                s = Shape(input_model, item, self, extras)
                s._index = idx
                shapes.append(s)
                idx = idx + 1
            input_model[_typestr] = "array"
        else:
            shapes = {}  
            if input_properties is not None:
                for k, v in input_properties.items():
                    if type(v) == dict and _propertiesstr in v:                            
                        dvalue = None
                        if k in self._data:
                            dvalue = data.get(k)
                        shapes[k] = Shape(v, dvalue, self, extras)

        self._shapes = shapes             

    def get_prop(self, prop):
        extras = self._extras
        shapes = self._shapes
        data = self._data
        parent = self._parent

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path[0] == "$":
                if path == "$parent":
                    return parent.get_prop(remaining_path)                
                                
                if extras:
                    if path == "$params":
                        return extras["$params"].get_prop(remaining_path)

                    if path == "$query":
                        return extras["$query"].get_prop(remaining_path)

                    if path == "$path":
                        return extras["$path"].get_prop(remaining_path)

            if self._array:
                idx = int(path[1:])
                return shapes[idx].get_prop(remaining_path)

            return shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":

                if prop == "$parent" or prop == "$length" or prop == "$index":
                    if prop == "$parent":
                        return parent
                    if prop == "$length":
                        return len(data)
                    if prop == "$index":
                        return self._index

                if extras:
                    if prop == "$params":
                        return extras["$params"]

                    if prop == "$query":
                        return extras["$query"]

                    if prop == "$path":
                        return extras["$path"]

            if self._array:
                idx = int(prop[1:])                
                return shapes[idx]

            if prop in shapes:
                return shapes[prop]

            if prop in data:
                return data[prop]

            if self._input_properties is not None and prop in self._input_properties:
                defaultstr = "default"
                input_type = self._input_properties[prop]
                if defaultstr in input_type:
                    return input_type[defaultstr]
            
            return None

    def set_prop(self, prop, value):
        shapes = self._shapes

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path[0] == "$":
                if path == "$params":
                    return self._extras["$params"].set_prop(remaining_path, value)

            if path in shapes:
                if self._array:
                    idx = int(path[1:])
                    return shapes[idx].set_prop(remaining_path, value)
                else:
                    return shapes[path].set_prop(remaining_path, value)
        else:            
            self._data[prop] = self.check_and_cast(prop, value)

    def validate(self):
        
        extras = self._extras        
        if extras:
            errors = []
            query = extras["$query"]
            path = extras["$path"]            
            
            t = query.validate()
            if t:
                errors.extend(t)
            t = path.validate()
            
            if t:
                errors.extend(t)
            
            if self._validator:
                t = list(self._validator.iter_errors(self._data))  
                if t:
                    errors.extend(t)

            return [{ "path" : e.path[0], "message" : e.message } for e in errors]
        else:
            if self._validator:            
                return list(self._validator.iter_errors(self._data))
    
    def check_and_cast(self, prop, value):
        if self._input_properties is not None and prop in self._input_properties:
                prop_schema = self._input_properties[prop]
                _type_str = "type"
                if _type_str in prop_schema:
                    ptype = prop_schema[_type_str]
                    try:
                        if ptype  == "integer" and not isinstance(value, int):
                            return int(value)
                        if ptype  == "string" and not isinstance(value, str):
                            return str(value)
                        if ptype  == "number" and not isinstance(value, float):
                            return float(value)
                    except:
                        pass                    
        return value

class ExecutionContextHelper:

    def __init__(self, parameter_rx):
        self._parameter_rx = parameter_rx
        self._cache = {}

    def get_executable_content(self, chr, query):
        exeutable_content_str = "executable_content"
        if exeutable_content_str in query:
            return query[exeutable_content_str]
        else:
            executable_content = self._parameter_rx.sub(chr, query["content"])
            query[exeutable_content_str] = executable_content
            return executable_content

    def build_parameters(self, query ,input_shape, get_value_converter):
        values = []
        _cache = self._cache

        if "parameters" in query:
            parameters = query["parameters"]    
            for p in parameters:
                pname = p["name"]
                ptype = p["type"]

                if pname in _cache:
                    pvalue = _cache[pname]
                else:
                    pvalue = input_shape.get_prop(pname)
                    
                    if "$params" not in pname:
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

class SQLiteExecutionContext:
    
    def __init__(self, root_path, db_name):        
        self._root_path = root_path
        db_path = root_path + "/db"
        self._con = sqlite3.connect(db_path + "/" + db_name)
        self._con.row_factory = self._sqlite_dict_factory
    
    def _sqlite_dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def get_value(self, ptype, value):        
        if ptype == "blob":        
            return sqlite3.Binary(value)        
        return value

    def execute(self, query, input_shape, helper):        
        con = self._con        
        content = helper.get_executable_content("?", query)                                       
        with con:
            cur = con.cursor()
            args = helper.build_parameters(query, input_shape, self.get_value)
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
        
    def execute(self, query, input_shape, helper):
        con = self._conn
        content = helper.get_executable_content("%s", query)
        
        with con:
            cur = con.cursor(cursor_factory = RealDictCursor)
            args = helper.build_parameters(query, input_shape, self.get_value_converter)
            cur.execute(content, args)
            rows = cur.fetchall()
            
        return rows, cur.lastrowid