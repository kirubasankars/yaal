import os
import re
import argparse
import copy
from collections import defaultdict

import yaml
import json

import sqlite3 as lite

from flask import Flask 
from flask import jsonify
from flask import request

class NodeExecutor:

    def __init__(self, node_descriptor, node_execution_builder, execution_context):
        self._node_descriptor = node_descriptor
        self._node_execution_builder = node_execution_builder
        self._execution_context = execution_context        
        self._nodes = {}

        self._input_model = node_descriptor.get_input_model()
        self._output_model = node_descriptor.get_output_model()
        
        self._input_type = None
        self._output_type = None
        self._output_partition_by = None
        self._parent_rows = None

        _typestr = "_type"
        _partitionbystr = "_partition_by"
        _parentrowsstr = "_parent_rows"
        if self._input_model is not None and _typestr in self._input_model:
            self._input_type = self._input_model[_typestr]
        
        if self._output_model is not None and _typestr in self._output_model:
            self._output_type = self._output_model[_typestr]
        
        if self._output_model is not None and _partitionbystr in self._output_model:
            self._output_partition_by = self._output_model[_partitionbystr]

        if self._output_model is not None and _parentrowsstr in self._output_model:
            self._parent_rows = self._output_model[_parentrowsstr]

        output_model = self._output_model

    def get_node_descritor(self):
        return self._node_descriptor

    def get_execution_context(self):
        return self._execution_context

    def get_output_type(self):
        return self._output_type

    def get_nodes(self):
        return self._nodes

    def _execute(self, input, parent_rows):
        execution_context =  self._execution_context
        node_descriptor = self.get_node_descritor()
        input_type = self._input_type
        output_partition_by = self._output_partition_by
    
        output = []        
        try:            
            execution_context.begin()

            if input_type is not None and input_type == "list":                
                output = execution_context.execute(self, input)   
                # loop on input
                # output.extend(execution_context.execute(self, input))
            elif input_type is None or input_type == "object":                
                output = execution_context.execute(self, input)
            
            if self._parent_rows == True:
                output = copy.deepcopy(parent_rows)

            nodes = self.get_nodes()
            if len(nodes) > 0:
                for sub_node_executor in nodes:
                    sub_node_descriptor = sub_node_executor.get_node_descritor()
                    sub_node_name = sub_node_descriptor.get_name()
                    sub_node_value = None
                    if input is not None:
                        sub_node_value = input.get_prop(sub_node_name)
                    sub_node_output = sub_node_executor._execute(sub_node_value, output)                    
        
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
            print(e)
            execution_context.error()  
            raise e
        finally:
            execution_context.end()
        
        return output

    def map(self, result):        
        mapped_result = []        
        
        for row in result:
            mapped_row = {}
            sub_mapped_nodes = {}
            nodes = self.get_nodes()
            if len(nodes) > 0:
                for sub_node_executor in nodes:
                    sub_node_descriptor = sub_node_executor.get_node_descritor()
                    sub_node_name = sub_node_descriptor.get_name()
                    
                    if sub_node_name in row:
                        sub_mapped_nodes[sub_node_name] = sub_node_executor.map(row[sub_node_name])

            output_model = self._output_model
            _typestr = "_type"
            _mappedstr = "_mapped"
            if output_model is not None:
                for k, v in output_model.items():
                    if type(v) == dict and (_typestr in v or _mappedstr in v):
                        if _mappedstr in v:
                            _mapped = v[_mappedstr]
                            mapped_row[k] = row[v["_mapped"]]
                        else:
                            if _typestr in v:
                                _type = v[_typestr]       
                                if _type == "list" or _type == "object":
                                    mapped_row[k] = sub_mapped_nodes[k]                        
            else:
                mapped_row = row

            for k, v in sub_mapped_nodes.items():
                mapped_row[k] = v

            mapped_result.append(mapped_row)

        if self._output_type == "object":
            mapped_result = mapped_result[0]

        return mapped_result

    def get_result(self, input):        
        rs = self._execute(input, [])
        rs = self.map(rs)        
        return rs

    def get_result_json(self, input):        
        return json.dumps(self.get_result(input), indent = 4)

    def set_nodes(self, nodes):
        self._nodes = nodes
    
    def get_nodes(self):
        return self._nodes 
    
    def create_input_shape(self, data):
        input_model = self._node_descriptor.get_input_model()
        return Shape(input_model, data, None) 

    def build(self):
        self._node_execution_builder.build(self)

class NodeExecutorBuilder:

    def build(self, node_executor):
        node_descriptor = node_executor.get_node_descritor()
        execution_context = node_executor.get_execution_context()

        node_descriptor_nodes = node_descriptor.get_nodes()
        if node_descriptor_nodes is not None:
            nodes = []
            for sub_node_descriptor in node_descriptor_nodes:
                sub_node_executor = NodeExecutor(sub_node_descriptor, self, execution_context)
                sub_node_executor.build()
                nodes.append(sub_node_executor)

            node_executor.set_nodes(nodes)

class NodeDescriptor:

    def __init__(self, name, method, path, root, node_descriptor_builder):
        self._name = name
        self._method = method
        self._path = path
        self._root = root
        self._parameters = None
        self._positional_parameters = None
        self._node_descriptor_builder = node_descriptor_builder

    def build(self, treemap, input_model, output_model):        
        self._node_descriptor_builder.build(self, treemap, input_model, output_model)

    def get_name(self):
        return self._name 

    def get_path(self):
        return self._path

    def get_method(self):
        return self._method

    def set_nodes(self, nodes):
        self._nodes = nodes

    def get_nodes(self):
        return self._nodes

    def set_content(self, content):
        self._content = content

    def get_content(self):
        return self._content

    def set_input_model(self, input_model):
        self._input_model = input_model
    
    def get_input_model(self):        
        return self._input_model

    def set_output_model(self, output_model):
        self._output_model = output_model
    
    def get_output_model(self):
        return self._output_model

    def set_parameters(self, parameters, positions):
        self._parameters = parameters
        self._positional_parameters = positions

    def get_parameters(self):
        return (self._parameters, self._positional_parameters)

    def create_executor(self, execution_context):
        node_execution_builder = NodeExecutorBuilder()
        node_executor = NodeExecutor(self, node_execution_builder, execution_context)
        node_executor.build()   
        return node_executor     

class NodeDescriptorParameter:
    def __init__(self, name, ptype):
        self._name = name
        self._ptype = ptype

    def get_name(self):
        return self._name

    def get_type(self):
        return self._ptype

class NodeDescritporBuilder:
    def __init__(self, content_reader):
        self._content_reader = content_reader
        self._parameters_meta_rx = re.compile("--\((.*)\)--")
        self._parameter_meta_rx = re.compile("([A-Za-z0-9_.$-]+)(\s+(\w+))?")
        self._parameter_rx = re.compile("\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)

    def parse_clean_parameters(self, node_descriptor):
        content = node_descriptor.get_content()
        lines = content.splitlines()

        if len(lines) == 0:
            return

        first_line = lines[0]
        parameters_meta_m = self._parameters_meta_rx.match(first_line)

        meta = {}
        if parameters_meta_m is not None:
            params_meta = parameters_meta_m.groups(1)[0].split(",")
            for p in params_meta:
                parameter_meta_m = self._parameter_meta_rx.match(p)

                if parameter_meta_m is not None:
                    gm = parameter_meta_m.groups(1)
                    if len(gm) > 0:
                        parameter_name = gm[0].lower()
                    if len(gm) > 1:
                        parameter_type = gm[2]                    
                    node_descriptor_parameter = NodeDescriptorParameter(parameter_name, parameter_type)
                    meta[parameter_name] = node_descriptor_parameter
            
            node_descriptor.set_parameters(meta, [x.lower() for x in self._parameter_rx.findall(content)])            
            node_descriptor.set_content("\r\n".join(lines[1:]))

    def build(self, node_descriptor, treemap, input_model, output_model):
        path = node_descriptor.get_path()
        method = node_descriptor.get_method()

        content = self._content_reader.get_sql(method, path)
        node_descriptor.set_content(content)

        if content is not None and content is not "":
            self.parse_clean_parameters(node_descriptor)
        
        if input_model is None:
            input_model = {
                "_type" : "object"
            }
        
        node_descriptor.set_input_model(input_model)        
        node_descriptor.set_output_model(output_model)

        sub_nodes_names = {}
        for k in treemap:            
            sub_nodes_names[k] = treemap[k]
        
        output_model = node_descriptor.get_output_model()
        _typestr = "_type"
        if output_model is not None:
            for k, v in output_model.items():
                if type(v) == dict and _typestr in v:                        
                    _type = v[_typestr]       
                    if _type == "list" or _type == "object":
                        sub_nodes_names[k] = {}
        
        nodes = []
        for k, v in sub_nodes_names.items():
            name = ".".join([method, k])
            sub_node_descriptor = NodeDescriptor(k, name, path, False, self)

            sub_input_model = None
            sub_output_model = None
            
            if k not in input_model:                
                input_model[k] = {
                    "_type" : "object"
                }
                sub_input_model = input_model[k]
                        
            if output_model is not None and k in output_model:
                sub_output_model = output_model[k]

            sub_node_descriptor.build(v, sub_input_model, sub_output_model)
            nodes.append(sub_node_descriptor)

        node_descriptor.set_nodes(nodes)

class NodeDescritporFactory:

    def __init__(self, content_reader, node_descriptor_builder):
        self._content_reader = content_reader
        self._node_descriptor_builder = node_descriptor_builder

    def _order_list_by_dots(self, files):        
        dots = [x.count(".") for x in files]
        ordered = []
        for x in range(0, len(dots)):
            if len(dots) == 0:
                break

            el = min(dots)
            while True:
                try:
                    idx = dots.index(el)
                    ordered.append(files[idx])
                    del files[idx]
                    del dots[idx]
                except:
                    break
        return ordered

    def _build_treemap(self, list):        
        map = {}
        for item in list:
            self._treemap(map, item)
        return map

    def _treemap(self, map, item):        
        if item == "":
            return
        item = item.lower()
        dot = item.find(".")
        if  dot > -1:
            path = item[0:dot]
            remaining_path = item[dot + 1:]
            if path not in map:
                map[path] = {}
            self._treemap(map[path], remaining_path)
        else:
           if item not in map:
               map[item] = {}

    def create(self, method, path):        
        ordered_files = self._order_list_by_dots(self._content_reader.list_sql(method, path))
        if len(ordered_files) == 0:
            return None
        treemap = self._build_treemap(ordered_files)
        
        config_str = self._content_reader.get_config(method, path)
        input_model_str = "input.model"
        output_model_str = "output.model"
        input_model = None
        output_model = None        
        if config_str is not None:
            config = yaml.load(config_str)
            if input_model_str in config:
                input_model = config[input_model_str]
            if output_model_str in config:
                output_model = config[output_model_str]

        node_descriptor = NodeDescriptor(method, method, path, True, self._node_descriptor_builder)
        node_descriptor.build(treemap[method], input_model, output_model)

        return node_descriptor
        
class GravityConfiguration:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

class FileReader:

    def __init__(self, gravity_configuration):
        self._gravity_configuration = gravity_configuration

    def get_sql(self, method, path):
        file_path = os.path.join(*[self._gravity_configuration.get_root_path(), path, method + ".sql"])
        return self._get(file_path)
    
    def get_config(self, method, path):
        file_path = os.path.join(*[self._gravity_configuration.get_root_path(), path, method + ".yml"])
        return self._get(file_path)

    def _get(self, file_path):        
        try:
            #print(file_path)
            with open(file_path, "r") as file:
                content = file.read()
        except:
            content = None
        return content

    def list_sql(self, method, path):        
        files = os.listdir(os.path.join(*[self._gravity_configuration.get_root_path(), path]))
        ffiles = [f.replace(".sql", "") for f in files if f.startswith(method) and f.endswith(".sql")]        
        return ffiles

class Gravity:

    def __init__(self, gravity_configuration):
        self._gravity_configuration = gravity_configuration
        self._content_reader = FileReader(self._gravity_configuration)
        self._node_descriptor_builder = NodeDescritporBuilder(self._content_reader)
        self._node_descriptor_factory = NodeDescritporFactory(self._content_reader, self._node_descriptor_builder)

    def create_descriptor(self, method, path):        
        return self._node_descriptor_factory.create(method, path)

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
            parameters, positional = node_descriptor.get_parameters()
            if parameters is not None:
                for p in positional:
                    args.append(str(input.get_prop(p)))
            cur.execute(content, [1])
            rows = cur.fetchall()

        return rows        

class Shape:
    def __init__(self, input_model, data, parent_shape):        
        self._list = False
        self._object = False

        self.data = data or {}
        self.shapes = {}
        self._parent = parent_shape
        self._input_model = input_model
        
        if input_model is None:
            return

        _typestr = "_type"
        if _typestr in input_model:
            _type = input_model[_typestr]
            if _type == "list":
                self._list = True                
            else:
                self._object = True
        
        if self._list and data is not None:            
            idx = 0
            input_model["_type"] = "object"
            for item in data:                
                self.shapes["@" + str(idx)] = Shape(input_model, item, self)                
                idx = idx + 1
            input_model["_type"] = "list"
        else:
            for k, v in input_model.items():
                if k != _typestr and type(v) == dict:
                    if _typestr in v:
                        _type = v[_typestr]
                        if _type == "list" or _type == "object":
                            dvalue = None
                            if data is not None and k in data:
                                dvalue = data.get(k)
                            self.shapes[k] = Shape(v, dvalue, self)

    def get_prop(self, prop):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path == "$parent":
                return self._parent.get_prop(remaining_path)

            if path in self.shapes:
                return self.shapes[path].get_prop(remaining_path)
        else:
            if prop == "$parent":
                return self._parent

            if prop == "$length":
                return len(self.data)

            if prop in self.shapes:
                return self.shapes[prop]

            if prop in self.data:
                return self.data[prop]
            
            return None

    def set_prop(self, prop, value):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path == "$parent":
                return self._parent.set_prop(remaining_path, value)

            if path in self.shapes:
                return self.shapes[path].set_prop(remaining_path, value)
        else:                       
            self.data[prop] = value

app = Flask(__name__)
gravity = Gravity(GravityConfiguration("/home/kirubasankars/workspace/gravity/serve"))
execution_context = SQLiteExecutionContext()

@app.route('/<app_name>/', defaults={'app_name':'','path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<app_name>/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def hello(app_name, path):    
    descriptor = gravity.create_descriptor(request.method.lower(), app_name + "/" + path)
    if descriptor is not None:        
        executor = descriptor.create_executor(execution_context)

        input_shape = None
        try:
            ijson = request.get_json()                    
            input_shape = executor.create_input_shape(ijson)
        except:
            input_shape = executor.create_input_shape(None)

        for k, v in request.args.items():
            input_shape.set_prop(k, v)
    
    return jsonify(executor.get_result(input_shape))

if __name__ == '__main__':
    app.run()
