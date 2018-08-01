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
                if v is not None:
                    if t == "integer":
                        v = int(v)
                    elif t == "string":
                        v = str(v)
                    else:
                        v = str(v)    
                values.append(v)

        return values  

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
        
        if input_model is None:
            input_model = {
                "type" : "object"
            }
        
        node_descriptor.set_input_model(input_model)        
        node_descriptor.set_output_model(output_model)

        sub_nodes_names = {}
        for k in treemap:            
            sub_nodes_names[k] = treemap[k]
        
        _propertiesstr = "properties"
        output_model = node_descriptor.get_output_model()        
        if output_model is not None:
            if _propertiesstr in output_model:
                for k, v in output_model[_propertiesstr].items():
                    if type(v) == dict and _propertiesstr in v:                        
                            sub_nodes_names[k] = {}
        
        nodes = []
        for k, v in sub_nodes_names.items():
            name = ".".join([method, k])
            sub_node_descriptor = NodeDescriptor(k, name, path, False, self)

            sub_input_model = None
            sub_output_model = None
            
            if k not in input_model:                
                input_model[k] = {
                    "type" : "object"
                }
                sub_input_model = input_model[k]
            
            if output_model is not None:
                if _propertiesstr in output_model:
                    if k in output_model[_propertiesstr]:
                        sub_output_model = output_model[_propertiesstr][k]

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
