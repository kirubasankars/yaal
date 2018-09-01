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

def _get_action_output(descriptor, data_providers, context, data_provider_helper):
    errors = []
    
    input_type, actions, data_provider = descriptor["input_type"], descriptor["actions"], data_providers["db"]
    paramsstr, headerstr, cookiestr, errorstr, breakstr = "$params", "$header", "$cookie", "$error", "$break"
    
    rs = []
    if actions is not None: 
        for action in actions:
            if "connection" in action:
                connection = action["connection"]        
                if connection in data_providers:
                    output, output_last_inserted_id = data_providers[connection].execute(action, context, data_provider_helper)
                else:
                    raise Exception("connection string " + connection + " missing")
            else:
                output, output_last_inserted_id = data_provider.execute(action, context, data_provider_helper)
            
            context.get_prop("$params").set_prop("$last_inserted_id", output_last_inserted_id)
            
            if len(output) >= 1:
                output0 = output[0]
                if errorstr in output0 or paramsstr in output0 or headerstr in output0 or cookiestr in output0 or breakstr in output0: 
                    if errorstr in output0 and output0[errorstr] == 1:
                        if "$http_status_code" in output0:
                            context.get_prop("$response.status_code", output0["$http_status_code"])                        
                        errors.extend(output)
                        return None, errors
                    elif paramsstr in output0 and output0[paramsstr] == 1:
                        params = context.get_prop(paramsstr)
                        for k, v in output0.items():
                            params.set_prop(k, v)
                    elif cookiestr in output0 and output0[cookiestr] == 1:                        
                        cookie = context.get_prop("$response.cookie")
                        for c in output:
                            if "name" in c and "value" in c:
                                cookie.set_prop(c["name"], c)
                    elif headerstr in output0 and output0[headerstr] == 1:
                        header = context.get_prop("$response.header")                        
                        for h in output:
                            if "name" in h and "value" in h:
                                header.set_prop(h["name"], h)
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
    return rs, None

def _execute_descriptor(descriptor, data_providers, context, parent_rows, parent_partition_by, data_provider_helper):
    input_type, output_partition_by = descriptor["input_type"], descriptor["partition_by"]
    use_parent_rows, execution_context =  descriptor["use_parent_rows"], data_providers["db"]
    
    root = ("root" in descriptor)    
    output = []

    try:
        if root:
            execution_context.begin()
            
            if "init" in descriptor:
                rs, errors =  _execute_descriptor(descriptor["init"], data_providers, context, [], output_partition_by, data_provider_helper)
                if errors:
                    return None, errors    

        if input_type is not None and input_type == "array":
            length = int(context.get_prop("$length"))
            for i in range(0, length):
                item_ctx = context.get_prop("@" + str(i))                    
                rs, errors = _get_action_output(descriptor, data_providers, item_ctx, data_provider_helper)
                output.extend(rs)
                if errors:
                    return None, errors
        
        elif input_type is None or input_type == "object":                
            output, errors = _get_action_output(descriptor, data_providers, context, data_provider_helper)
            if errors:
                return None, errors
            if use_parent_rows == True and parent_partition_by is None:
                raise Exception("parent _partition_by is can't be empty when child wanted to use parent rows")
            
            if use_parent_rows == True:
                output = copy.deepcopy(parent_rows)

            branches = descriptor["branches"]
            if branches:
                for branch_descriptor in branches:                    
                    sub_node_name = branch_descriptor["name"]                    
                    sub_node_shape = None
                    if context is not None:
                        sub_node_shape = context.get_prop(sub_node_name)

                    sub_node_output, errors = _execute_descriptor(branch_descriptor, data_providers, sub_node_shape, output, output_partition_by, data_provider_helper)                    
                    if errors:
                        return None, errors

                    if not descriptor["actions"] and not output:
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
    
    return output, None

def _output_mapper(output_type, output_modal, branches, result):
    mapped_result = []
     
    _typestr, _mappedstr = "type", "mapped" 

    output_model = output_modal
    if output_model and "properties" in output_model:
        output_properties = output_model["properties"]
    else:
        output_properties = None
    
    output_type = output_type

    for row in result:
        mapped_obj = {}
        mapped_tree = {}        
        if branches:
            for branch_descriptor in branches:                    
                
                branch_name = branch_descriptor["name"]
                branch_output_type = branch_descriptor["output_type"]

                if output_properties and branch_name in output_properties: 
                    branch_output_model = output_properties[branch_name]
                else:
                    branch_output_model = None

                branch_descriptor_branches = branch_descriptor["branches"]

                if branch_name in row:
                    mapped_tree[branch_name] = _output_mapper(branch_output_type, branch_output_model, branch_descriptor_branches, row[branch_name])
        
                                        
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

def _build_descriptor_action(descriptor, content):
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
        descriptor["parameters"] = meta
    else:
        descriptor["parameters"] = None

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

                _build_action_parameters(action, descriptor)
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
        _build_action_parameters(action, descriptor)
        actions.append(action)

    descriptor["actions"] = actions

def _build_action_parameters(action, descriptor):
    content = action["content"]
    parameter_names = [x.lower() for x in parameter_rx.findall(content)]
    descriptor_parameters = descriptor["parameters"]

    params = []
    for p in parameter_names:
        action_parameter = None
        if descriptor_parameters is not None and p in descriptor_parameters:
            descriptor_parameter = descriptor_parameters[p]
            action_parameter = {
                "name": p,
                "type": descriptor_parameter["type"]
            }
            params.append(action_parameter)                            
        else:
            raise TypeError("type missing for {{" + p + "}} in the " + descriptor["method"] + ".sql")
    
    if len(params) != 0:
        action["parameters"] = params    

def _build_descriptor(descriptor, root, tree_map, content_reader, input_model, output_model):
    _propertiesstr, _typestr, _partitionbystr, _parentrowsstr = "properties", "type", "partition_by", "parent_rows"
    _outputtypestr, _useparentrowsstr, _parametersstr, _actionsstr = "output_type", "use_parent_rows", "parameters", "actions"

    path, method = descriptor["path"], descriptor["method"]    
    content = content_reader.get_sql(method, path)
    branch_map = {}
    
    if input_model is None:
        input_model = {
            "type" : "object",
            _propertiesstr : {}
        }
    if _propertiesstr not in input_model:
        input_model[_propertiesstr] = {}

    if root:
        descriptor["input_model"] = input_model
        
    descriptor["input_type"] = input_model[_typestr]
    input_properties = input_model[_propertiesstr]
    
    if root:
        descriptor["output_model"] = output_model or { _typestr : "array", _propertiesstr : {} }
        
        before_descriptor = {
            "path" : "api",
            "method" : "init"
        }
        _build_descriptor(before_descriptor, False, {}, content_reader, None, None)

        if _actionsstr in before_descriptor and before_descriptor[_actionsstr]:
            descriptor["init"] = before_descriptor

    output_properties = None      
    if output_model is not None:
        if _typestr in output_model:
            descriptor[_outputtypestr] = output_model[_typestr]
        else:
            descriptor[_outputtypestr] = "array"

        if _propertiesstr in output_model:
            output_properties = output_model[_propertiesstr]            

        if _parentrowsstr in output_model:
            descriptor[_useparentrowsstr] = output_model[_parentrowsstr]
        else:
            descriptor[_useparentrowsstr] = None

        if _partitionbystr in output_model:
            descriptor[_partitionbystr] = output_model[_partitionbystr]
        else:
            descriptor[_partitionbystr] = None
                
        if output_properties is not None:
            for k in output_properties:
                v = output_properties[k]
                if type(v) == dict and _typestr in v:
                    _type = v[_typestr]
                    if _type == "object" or _type == "array":                      
                        branch_map[k] = {}
    else:
        descriptor[_outputtypestr] = "array"        
        descriptor[_useparentrowsstr] = False
        descriptor[_partitionbystr] = None

    _build_descriptor_action(descriptor, content)

    if _parametersstr not in descriptor:
        descriptor[_parametersstr] = None

    if _actionsstr not in descriptor:
        descriptor[_actionsstr] = None

    for k in tree_map:
        if k not in branch_map:
            branch_map[k] = tree_map[k]

    branches = []
    for branch_name in branch_map:

        branch = branch_map[branch_name]
        branch_method_name = ".".join([method, branch_name])
        branch_descriptor = {
            "name" : branch_name,
            "method" : branch_method_name,
            "path" : path
        }        

        branch_input_model = None
        branch_output_model = None
                    
        if branch_name not in input_properties:
            input_properties[branch_name] = {
                _typestr : "object",
                _propertiesstr : {}
            }
        branch_input_model = input_properties[branch_name]
        
        if output_properties is not None:                
            if branch_name in output_properties:
                branch_output_model = output_properties[branch_name]

        _build_descriptor(branch_descriptor, False, branch, content_reader, branch_input_model, branch_output_model)
        branches.append(branch_descriptor)
    
    if len(branches) != 0:
        descriptor["branches"] = branches
    else:
        descriptor["branches"] = None

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

def _default_date_time_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def get_result(descriptor, data_providers, ctx):
    
    errors = ctx.get_prop("$request").validate()
    
    statuscodestr = "$response.status_code"
    if errors:
        ctx.set_prop(statuscodestr, 400)
        return { "errors" : errors }

    try:        
        if "db" not in data_providers:
            raise Exception("default connection string name db is missing")
        
        data_provider_helper = DataProviderHelper(parameter_rx)        
        rs, errors = _execute_descriptor(descriptor, data_providers, ctx, [], None, data_provider_helper)
        
        if errors:
            status_code = ctx.get_prop(statuscodestr)
            if not status_code:
                ctx.set_prop(statuscodestr, 400)
            return { "errors" : errors }

        rs = _output_mapper(descriptor["output_type"], descriptor["output_model"], descriptor["branches"], rs)
        ctx.set_prop(statuscodestr, 200)
        
        return rs
    except Exception as e:        
        return { "errors" : e.args[0] }

def get_result_json(descriptor, data_providers, context):
    return json.dumps(get_result(descriptor, data_providers, context), default= _default_date_time_converter)

def get_descriptor_json(descriptor):
    d = copy.deepcopy(descriptor)
    del d["_validators"]
    return json.dumps(d)    

namespaces = {}
def get_namespace(name, root_path, debug):
    if not debug and name in namespaces:
        return namespaces[name]
    else:
        root_path = os.path.join(*[root_path, name])
        namespaces[name] = Gravity(root_path, None, debug)         
        return namespaces[name]

def create_context(descriptor, body, params, query, header, cookie):    
    
    validators = descriptor["_validators"]
    
    querystr, headerstr, cookiestr, modelstr = "input_query", "input_header", "input_cookie", "input_model"

    if  querystr in descriptor:
        query_schema = descriptor[querystr]
        query_validator = validators[querystr]
    else:
        query_schema = None
        query_validator = None

    if headerstr in descriptor:
        header_schema = descriptor[headerstr]
        header_validator = validators[headerstr]
    else:
        header_schema = None
        header_validator = None

    if cookiestr in descriptor:
        cookie_schema = descriptor[cookiestr]
        cookie_validator = validators[cookiestr]
    else:
        cookie_schema = None
        cookie_validator = None

    if modelstr in descriptor:
        model_schema = descriptor[modelstr]
        model_validator = validators[modelstr]
    else:
        model_schema = None
        model_validator = None
    
    query_shape = Shape(query_schema, query, None, None, query_validator)
    if query:
        for k, v in query.items():
            query_shape.set_prop(k, v)
    
    header_shape = Shape(header_schema, header, None, None, header_validator)
    cookie_shape = Shape(cookie_schema, cookie, None, None, cookie_validator)

    request_extras = {
        "$query" : query_shape,
        "$header" : header_shape,
        "$cookie" : cookie_shape
    }
    request_shape = Shape({}, None, None, request_extras, None)
    
    response_extras = {
        "$header" :  Shape(None, None, None, None, None),
        "$cookie" : Shape(None, None, None, None, None)
    }
    response_shape = Shape({}, None, None, response_extras, None)

    params_shape = Shape({}, params, None, None, None)

    extras = {
        "$params" : params_shape,
        "$query" : query_shape,
        "$header" : header_shape,
        "$cookie" : cookie_shape,
        "$request" : request_shape,
        "$response" : response_shape
    }

    return Shape(model_schema, body, None, extras, model_validator)

def create_descriptor(method, path, content_reader):
    path = "api/" + path 
    ordered_files = _order_list_by_dots(content_reader.list_sql(method, path))       
    if len(ordered_files) == 0: 
        return None # found zero sql files, then return no api available

    treemap = _build_treemap(ordered_files)        
    config = content_reader.get_config(method, path)
    
    input_model_str, output_model_str, input_body_str, input_query_str = "input.model", "output.model", "body", "query"    
    input_model, output_model, input_query, input_path = None, None, None, None
    
    if config is not None:
        if input_model_str in config:
            input_config = config[input_model_str]
            if input_config is not None:
                if input_body_str in input_config:
                    input_model = input_config[input_body_str]
                if input_query_str in input_config:
                    input_query = input_config[input_query_str]               
            
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

    descriptor = {
        "name" : method,
        "method" : method,
        "path" : path,        
        "root": True,
        "input_query" : input_query
    }
    
    if input_query:
        input_query_schema_validator = Draft4Validator(schema = input_query, format_checker = FormatChecker())
    else:
        input_query_schema_validator = None

    if input_model:
        input_model_schema_validator = Draft4Validator(schema = input_model, format_checker = FormatChecker())
    else:
        input_model_schema_validator = None

    _build_descriptor(descriptor, True, treemap[method], content_reader, input_model, output_model)
    validators = {
        "input_query" : input_query_schema_validator,
        "input_model" : input_model_schema_validator
    }
    descriptor["_validators"] = validators
    return descriptor

class Shape:
    
    def __init__(self, input_model, data, parent_shape, extras, validator):        
        self._array = False
        self._object = False

        self._data = data or {}
        self._parent = parent_shape        
        self._input_model = input_model
        self._input_properties = None
        input_properties = None
        self._index = 0
        self._validator = validator
        
        self._extras = extras        
        
        if data is not None and ("$parent" in data or "$length" in data):
            raise Exception("$parent or $length is reversed keywords. You can't use them.")

        input_model = input_model or {}
        
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
                s = Shape(input_model, item, self, extras, None)
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
                        shapes[k] = Shape(v, dvalue, self, extras, None)

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
                    if path in extras:
                        return extras[path].get_prop(remaining_path)
                    
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
                    if prop in extras:
                        return extras[prop]
                    
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
            
            if path in shapes:
                if self._array:
                    idx = int(path[1:])
                    return shapes[idx].set_prop(remaining_path, value)
                else:
                    return shapes[path].set_prop(remaining_path, value)
            else:
                if path in self._extras:
                    self._extras[path].set_prop(remaining_path, value)
        else:            
            self._data[prop] = self.check_and_cast(prop, value)

    def validate(self):
        errors = []
        extras = self._extras        
        if extras:
           
            for name, extra in extras.items():                                    
                for x in extra.validate():                                    
                    errors.append(x)            
            
        if self._validator:
            errorlist = list(self._validator.iter_errors(self._data))  
            if errorlist:
                for x in errorlist:
                    p = None
                    if len(x.path) > 0:
                        p = x.path[0]
                    
                    m = {
                        "path" : p,
                        "message" : x.message                                                    
                    }
                    errors.append(m)

        return errors
       
                
    def get_data(self):
        return self._data

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

class DataProviderHelper:

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

class SQLiteDataProvider:
    
    def __init__(self, root_path, db_name):        
        self._root_path = root_path
        self._db_path = root_path + "/db/" + db_name        
        self._con = sqlite3.connect(self._db_path)
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

    def __init__(self, root_path, content_reader, debug):
        self._descriptors = {}
        self._descriptor_validators = {}
        self._root_path = root_path
        self._debug = debug or False
        
        if content_reader is None:
            self._content_reader = FileContentReader(self._root_path)
        else:
            self._content_reader = content_reader

    def get_data_providers(self):        
        data_providers = {
            "db" : PostgresDataProvider(self._root_path, "dvdrental"),
            "sqlite3" : SQLiteDataProvider(self._root_path, "sqlite3.db"),
            "app.db": SQLiteDataProvider(self._root_path, "app.db")
        }
        return data_providers

    def create_descriptor(self, method, path):        
        return create_descriptor(method, path, self._content_reader)
    
    def get_descriptor(self, method, path):
        k = os.path.join(*[path, method])
        if self._debug == False and k in self._descriptors:
            return self._descriptors[k]
        
        descriptor  = self.create_descriptor(method, path) 
        self._descriptors[k] = descriptor      
        return descriptor

class PostgresDataProvider:

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