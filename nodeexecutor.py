import json
import copy
from collections import defaultdict

from shape import Shape

class NodeExecutor:

    def __init__(self, node_descriptor, node_execution_builder, execution_context):
        self._node_descriptor = node_descriptor
        self._node_execution_builder = node_execution_builder
        self._execution_context = execution_context        
        self._nodes = {}
        self._output_type = None

    def get_node_descritor(self):
        return self._node_descriptor

    def get_execution_context(self):
        return self._execution_context

    def get_output_type(self):
        return self._output_type

    def _execute(self, input_shape, parent_rows, parent_partition_by):
        execution_context =  self._execution_context
        node_descriptor = self.get_node_descritor()
        input_type = node_descriptor.get_input_type()        
        output_partition_by = node_descriptor.get_partition_by()
        use_parent_rows = node_descriptor.get_use_parent_rows()

        output = []
        try:            
            execution_context.begin()

            if input_type is not None and input_type == "array":

                length = int(input_shape.get_prop("$length"))
                for i in range(0, length):
                    
                    item_input_shape = input_shape.get_prop("@" + str(i))
                    sub_output = execution_context.execute(self, item_input_shape)
                    if len(sub_output) == 1:
                        output0 = sub_output[0]
                        if "params" in output0 and output0["params"] == 1:
                            for k, v in output0.items():
                                item_input_shape.set_prop(k, v)
                        else:
                            output.extend(sub_output)
            
            elif input_type is None or input_type == "object":

                output = execution_context.execute(self, input_shape)                
                if len(output) == 1:
                    output0 = output[0]
                    if "params" in output0 and output0["params"] == 1:
                        for k, v in output[0].items():
                            input_shape.set_prop(k, v)                
            
            if use_parent_rows == True and parent_partition_by is None:
                raise Exception("parent _partition_by is can't be empty when child wanted to use parent rows")
            
            if use_parent_rows == True:
                output = copy.deepcopy(parent_rows)

            nodes = self.get_nodes()
            if len(nodes) > 0:
                for sub_node_executor in nodes:
                    sub_node_descriptor = sub_node_executor.get_node_descritor()
                    sub_node_name = sub_node_descriptor.get_name()                    
                    sub_node_shape = None
                    if input_shape is not None:
                        sub_node_shape = input_shape.get_prop(sub_node_name)
                    sub_node_output = sub_node_executor._execute(sub_node_shape, output, output_partition_by)                    

                    content = node_descriptor.get_content()
                    if (content is None or content == '') and len(output) == 0:
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
            print(e)
            execution_context.error()  
            raise e
        finally:
            execution_context.end()
        
        return output

    def map(self, result):        
        mapped_result = []        
        
        output_properties = self._node_descriptor.get_output_properties()
        output_type = self._node_descriptor.get_output_type()

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

    def get_result(self, input_shape):
        try:
            rs = self._execute(input_shape, [], None)
            rs = self.map(rs)        
            return rs
        except Exception as e:
            #raise e
            return { "errors" : [ { "message" : e.args[0] } ] }
            
    def get_result_json(self, input_shape):        
        return json.dumps(self.get_result(input_shape), indent = 4)

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
