import json

from shape import Shape

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
                prop_count = 0
                for k, v in output_model.items():
                    if type(v) == dict and (_typestr in v or _mappedstr in v):
                        if _mappedstr in v:
                            _mapped = v[_mappedstr]
                            mapped_row[k] = row[v["_mapped"]]
                            prop_count = prop_count + 1
                        else:
                            if _typestr in v:
                                _type = v[_typestr]       
                                if _type == "list" or _type == "object":
                                    mapped_row[k] = sub_mapped_nodes[k]                    
                    #if prop_count == 0:
                    #    mapped_row = row
            else:
                mapped_row = row

            for k, v in sub_mapped_nodes.items():
                mapped_row[k] = v

            mapped_result.append(mapped_row)

        if self._output_type == "object":
            if len(mapped_result) > 0:
                mapped_result = mapped_result[0]
            else:
                mapped_result = {}
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
