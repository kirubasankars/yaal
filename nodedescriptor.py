import re

from nodeexecutor import NodeExecutorBuilder
from nodeexecutor import NodeExecutor

class NodeDescriptor:

    def __init__(self, name, method, path, parent_node_descriptor, root, node_descriptor_builder):
        self._name = name
        self._method = method
        self._path = path
        self._parent_node_descriptor = parent_node_descriptor
        self._root = root
        self._parameters = None    
        self._node_descriptor_builder = node_descriptor_builder
        self._input_type = None
        self._output_type = None
        self._output_model = None
        self._input_model = None
        self._output_properties = None
        self._input_properties = None
        self._partition_by = None
        self._nodes = None
        self._node_queries = None
        self._use_parent_rows = None
        self._input_query = None
        self._input_path = None

    def build(self, treemap, input_model, output_model, input_query, input_path):        
        self._node_descriptor_builder.build(self, treemap, input_model, output_model, input_query, input_path)

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

    def set_input_type(self, input_type):
        self._input_type = input_type
    
    def get_input_type(self):        
        return self._input_type

    def set_output_type(self, output_type):
        self._output_type = output_type
    
    def get_output_type(self):        
        return self._output_type

    def set_input_properties(self, input_properties):
        self._input_properties = input_properties
    
    def get_input_properties(self):        
        return self._input_properties

    def set_output_properties(self, output_properties):
        self._output_properties = output_properties
    
    def get_output_properties(self):        
        return self._output_properties

    def set_output_model(self, output_model):
        self._output_model = output_model
    
    def get_output_model(self):
        return self._output_model

    def set_partition_by(self, partition_by):
        self._partition_by = partition_by
    
    def get_partition_by(self):
        return self._partition_by

    def set_use_parent_rows(self, use_parent_rows):
        self._use_parent_rows = use_parent_rows
    
    def get_use_parent_rows(self):
        return self._use_parent_rows

    def set_parameters(self, parameters):
        self._parameters = parameters

    def get_parameters(self):
        return self._parameters

    def get_node_queries(self):
        return self._node_queries

    def set_node_queries(self, queries):
        self._node_queries = queries

    def set_input_query(self, input_query):
        self._input_query = input_query

    def get_input_query(self):
        return self._input_query

    def set_input_path(self, input_path):
        self._input_path = input_path

    def get_input_path(self):
        return self._input_path

    def create_executor(self):
        node_execution_builder = NodeExecutorBuilder()
        node_executor = NodeExecutor(self, node_execution_builder)
        node_executor.build()
        return node_executor

    def get_input_propery_definition(self, prop):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if self._input_type == "array" and path == "$parent" and remaining_path.startswith("$parent"):
                remaining_path = remaining_path[remaining_path.find(".")+1:]

            if path == "$parent":
                return self._parent_node_descriptor.get_input_propery_definition(remaining_path)
            else:
                return None
        else:
            if prop == "$parent":
                return self._parent_node_descriptor

            if self._input_properties and prop in self._input_properties:
                return self._input_properties[prop]
                
            return None

class NodeQuery:
    
    def __init__(self, content, node_parameter_rx, node_descriptor, connection_name, notnullable):
        self._content = content
        self._executable_content = content
        self._node_descriptor = node_descriptor
        self._parameters = None
        self._connection_name = connection_name
        self._node_parameter_rx = node_parameter_rx
    
    def get_connection_name(self):
        return self._connection_name

    def get_parameters(self):
        return self._parameters
    
    def set_parameters(self, parameters):
        self._parameters = parameters

    def get_content(self):
        return self._content

    def get_node_descriptor(self):
        return self._node_descriptor
    
    def get_executable_content(self, chr):
        return self._node_parameter_rx.sub(chr, self.get_content())

    def build(self):

        parameter_names = [x.lower() for x in self._node_parameter_rx.findall(self._content)]
        node_descriptor_parameters = self._node_descriptor.get_parameters()

        params = []
        for p in parameter_names:
            node_query_parameter = None
            if node_descriptor_parameters is not None and p in node_descriptor_parameters:
                node_descriptor_parameter = node_descriptor_parameters[p]
                node_query_parameter = NodeQueryParameter(p, node_descriptor_parameter.get_type(), node_descriptor_parameter.get_property_def())
                params.append(node_query_parameter)                            
            else:
                raise TypeError("type missing for {{" + p + "}} in the " + self._node_descriptor.get_method() + ".sql")

        self.set_parameters(params)
    
    def build_parameter_values(self, input_shape):
        values = []
        _cache = {}
        if self._parameters is not None:
            for p in self._parameters:
                pname = p.get_name()
                ptype = p.get_type()
                prequired = p.get_is_required()

                if pname in _cache:
                    pvalue = _cache[pname]
                else:
                    pvalue = input_shape.get_prop(pname)
                    _cache[pname] = pvalue
                    
                if prequired == True and pvalue is None:
                    raise Exception("parameter " + pname + " is required. can't be none.")

                try:
                    if ptype == "integer":
                        pvalue = int(pvalue)
                    elif ptype == "string":
                        pvalue = str(pvalue)
                    else:
                        pvalue = str(pvalue)
                    values.append(pvalue)
                except:
                    if prequired == True:
                        raise Exception("parameter " + pname + " should be " + ptype + ", given value is " + str(pvalue))
                    else:
                        values.append(pvalue)
        return values
        
class NodeQueryParameter:
    
    def __init__(self, name, ptype, property_def):
        self._name = name
        self._ptype = ptype
        self._required = False
        self._prop_def = property_def

        _requiredstr = "required"
        if property_def and _requiredstr in property_def:
            if property_def[_requiredstr] == True:
                self._required = True

    def get_name(self):
        return self._name

    def get_type(self):
        return self._ptype

    def get_property_def(self):
        return self._prop_def

    def get_is_required(self):
        return self._required

class NodeDescriptorParameter:
    
    def __init__(self, name, ptype, property_def):
        self._name = name
        self._ptype = ptype
        self._required = False
        self._prop_def = property_def

        _requiredstr = "required"
        if property_def and _requiredstr in property_def:
            if property_def[_requiredstr] == True:
                self._required = True

    def get_name(self):
        return self._name

    def get_type(self):
        return self._ptype
    
    def get_property_def(self):
        return self._prop_def

    def get_is_required(self):
        return self._required

parameters_meta_rx = re.compile(r"--\((.*)\)--")
parameter_meta_rx = re.compile(r"\s*([A-Za-z0-9_.$-]+)(\s+(\w+))?\s*")
parameter_rx = re.compile(r"\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)
query_rx = re.compile(r"--query\(([a-zA-Z0-9.$_,]*?)\)\(([a-zA-Z0-9.$_]*?)\)--")

class NodeDescritporBuilder:

    def __init__(self, content_reader):
        self._content_reader = content_reader

    def build_node_quries(self, node_descriptor):
        
        global parameters_meta_rx
        global parameter_meta_rx
        global parameter_rx
        global query_rx

        content = node_descriptor.get_content()        
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
                    prop_def = node_descriptor.get_input_propery_definition(parameter_name)
                    node_descriptor_parameter = NodeDescriptorParameter(parameter_name, parameter_type, prop_def)
                    meta[parameter_name] = node_descriptor_parameter
            
            node_descriptor.set_parameters(meta)

        node_queries = []
        content = ""        
        connection_name = None
        notnullable = None

        for idx, line in enumerate(lines):
            if idx == 0 and parameters_first_line_m is not None:
                continue
            
            query_match = query_rx.match(line)
            if query_match is not None:                
                if content.lstrip().rstrip() is not "":        
                    node_query = NodeQuery(content, parameter_rx, node_descriptor, connection_name, notnullable)
                    node_query.build()
                    node_queries.append(node_query)
                
                connection_name = query_match.groups(0)[1].lstrip().rstrip()
                notnullable = query_match.groups(0)[0].lstrip().rstrip().split(",")
                if connection_name == "":
                    connection_name = None

                content = ""
            else:
                content = "\r\n".join([content, line])            
        
        if content.lstrip().rstrip() is not "":        
            node_query = NodeQuery(content, parameter_rx, node_descriptor, connection_name, notnullable)
            node_query.build()
            node_queries.append(node_query)
        
        node_descriptor.set_node_queries(node_queries)

    def build(self, node_descriptor, treemap, input_model, output_model, input_query, input_path):
        path = node_descriptor.get_path()
        method = node_descriptor.get_method()

        content = self._content_reader.get_sql(method, path)
        node_descriptor.set_content(content)

        sub_nodes_names = {}
        
        _propertiesstr = "properties"
        _typestr = "type"
        _partitionbystr = "partition_by"
        _parentrowsstr = "parent_rows"
        if input_model is None:
            input_model = {
                "type" : "object",
                "properties" : {}
            }
        if _propertiesstr not in input_model:
            input_model["properties"] = {}
        
        node_descriptor.set_input_type(input_model[_typestr])
        
        node_descriptor.set_input_query(input_query)
        node_descriptor.set_input_path(input_path)

        node_descriptor.set_input_model(input_model)        
        node_descriptor.set_output_model(output_model)
        
        input_properties = input_model[_propertiesstr]
        node_descriptor.set_input_properties(input_properties)
        
        self.build_node_quries(node_descriptor)

        output_properties = None        
        if output_model is not None:
            if _typestr in output_model:
                output_type = output_model[_typestr]
                node_descriptor.set_output_type(output_type)
                        
            if _propertiesstr in output_model:
                output_properties = output_model[_propertiesstr]
                node_descriptor.set_output_properties(output_properties)

            if _parentrowsstr in output_model:
                node_descriptor.set_use_parent_rows(output_model[_parentrowsstr])

            if _partitionbystr in output_model:
                node_descriptor.set_partition_by(output_model[_partitionbystr])
                   
            if output_properties is not None:
                for k in output_properties:
                    v = output_properties[k]
                    if type(v) == dict and _typestr in v:
                        _type = v[_typestr]
                        if _type == "object" or _type == "array":                      
                            sub_nodes_names[k] = {}

        for k in treemap:
            if k not in sub_nodes_names:
                sub_nodes_names[k] = treemap[k]

        nodes = []
        for k in sub_nodes_names:
            v = sub_nodes_names[k]
            name = ".".join([method, k])
            sub_node_descriptor = NodeDescriptor(k, name, path, node_descriptor, False, self)

            sub_input_model = None
            sub_output_model = None
                        
            if k not in input_properties:
                input_properties[k] = {
                    "type" : "object",
                    "properties" : {}
                }
            sub_input_model = input_properties[k]
            
            if output_properties is not None:                
                if k in output_properties:
                    sub_output_model = output_properties[k]

            sub_node_descriptor.build(v, sub_input_model, sub_output_model, input_query, input_path)
            nodes.append(sub_node_descriptor)

        node_descriptor.set_nodes(nodes)

class NodeDescritporFactory:

    def __init__(self, content_reader, node_descriptor_builder):
        self._content_reader = content_reader
        self._node_descriptor_builder = node_descriptor_builder

    def _order_list_by_dots(self, files):
        if files is None:
            return []

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

    def _build_treemap(self, filelist):        
        map = {}
        if filelist is not None:
            for item in filelist:
                self._treemap(map, item)
        return map

    def _treemap(self, map, item):        
        if item == "":
            return
        item = item
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
            return None # found zero sql files, then return no api available

        treemap = self._build_treemap(ordered_files)        
        config = self._content_reader.get_config(method, path)
        
        input_model_str = "input.model"
        output_model_str = "output.model"
        input_body_str = "body"
        input_query_str = "query"
        input_path_str = "path"
        
        input_query = None
        input_path = None
        input_model = None
        output_model = None

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
        
        node_descriptor = NodeDescriptor(method, method, path, None, True, self._node_descriptor_builder)
        node_descriptor.build(treemap[method], input_model, output_model, input_query, input_path)

        return node_descriptor
