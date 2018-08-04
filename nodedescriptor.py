import re
import yaml

from nodeexecutor import NodeExecutorBuilder
from nodeexecutor import NodeExecutor

class NodeDescriptor:

    def __init__(self, name, method, path, root, node_descriptor_builder):
        self._name = name
        self._method = method
        self._path = path
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
        self._use_parent_rows = None

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

    def create_executor(self, execution_context):
        node_execution_builder = NodeExecutorBuilder()
        node_executor = NodeExecutor(self, node_execution_builder, execution_context)
        node_executor.build()
        return node_executor

    def build_parameter_values(self, input_shape):
        values = []
        if self._parameters is not None:
            for p in self._parameters:
                n = p.get_name()
                t = p.get_type()

                v = input_shape.get_prop(n)
                try:
                    if t == "integer":
                        v = int(v)
                    elif t == "string":
                        v = str(v)
                    else:
                        v = str(v)
                    values.append(v)
                except Exception as e:
                    raise Exception(n + " should be " + t + ", given value is " + str(v))
        return values  

    def get_executable_content(self, sub_chr):
        self._parameter_rx = re.compile("\{\{([A-Za-z0-9_.$-]*?)\}\}", re.MULTILINE)
        return self._parameter_rx.sub("?", self._content)


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
        self._parameter_meta_rx = re.compile("\s*([A-Za-z0-9_.$-]+)(\s+(\w+))?\s*")
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
                    meta[parameter_name.lower()] = node_descriptor_parameter
            
            parameters = [meta.get(x.lower()) for x in self._parameter_rx.findall(content)]
            node_descriptor.set_parameters(parameters)            
            node_descriptor.set_content("\r\n".join(lines[1:]))

    def build(self, node_descriptor, treemap, input_model, output_model):
        path = node_descriptor.get_path()
        method = node_descriptor.get_method()

        content = self._content_reader.get_sql(method, path)
        node_descriptor.set_content(content)

        if content is not None and content is not "":            
            self.parse_clean_parameters(node_descriptor)

        sub_nodes_names = {}
        for k in treemap:            
            sub_nodes_names[k] = treemap[k]        

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

        node_descriptor.set_input_model(input_model)        
        node_descriptor.set_output_model(output_model)
        
        input_properties = input_model[_propertiesstr]
        node_descriptor.set_input_properties(input_properties)
        
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
                for k, v in output_properties.items():
                    if type(v) == dict and _propertiesstr in v:                        
                        sub_nodes_names[k] = {}

        nodes = []
        for k, v in sub_nodes_names.items():
            name = ".".join([method, k])
            sub_node_descriptor = NodeDescriptor(k, name, path, False, self)

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

            sub_node_descriptor.build(v, sub_input_model, sub_output_model)
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

    def _build_treemap(self, list):        
        map = {}
        for item in list:
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
            return None
        treemap = self._build_treemap(ordered_files)
        
        config_str = self._content_reader.get_config(method, path)
        input_model_str = "input.model"
        output_model_str = "output.model"
        input_model = None
        output_model = None        
        if config_str is not None and config_str != "":
            config = yaml.load(config_str)
            if input_model_str in config:
                input_model = config[input_model_str]
            if output_model_str in config:
                output_model = config[output_model_str]

        node_descriptor = NodeDescriptor(method, method, path, True, self._node_descriptor_builder)
        node_descriptor.build(treemap[method], input_model, output_model)

        return node_descriptor
