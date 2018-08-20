import json
import copy
import datetime
from collections import defaultdict

from shape import Shape

class NodeExecutor:

    def __init__(self, node_descriptor, node_execution_builder):
        self._node_descriptor = node_descriptor
        self._node_execution_builder = node_execution_builder        
        self._nodes = {}
        self._output_type = None

    def get_node_descritor(self):
        return self._node_descriptor

    def get_output_type(self):
        return self._output_type

    def e(self, input_type, node_descriptor, execution_contexts, input_shape):        
        node_queries = node_descriptor.get_node_queries()
        execution_context = execution_contexts["db"]
        paramsstr = "$params"
        errorstr = "$error"
        breakstr = "$break"
        rs = []
        if node_queries is not None: 
            for node_query in node_descriptor.get_node_queries():                            
                query_connection = node_query.get_connection_name()
                if query_connection is not None:
                    if query_connection in execution_contexts:
                        output, output_last_inserted_id = execution_contexts[query_connection].execute(node_query, input_shape)
                    else:
                        raise Exception("connection string " + query_connection + " missing")
                else:
                    output, output_last_inserted_id = execution_context.execute(node_query, input_shape)

                input_shape.set_prop("$last_inserted_id", output_last_inserted_id)

                if len(output) >= 1:
                    output0 = output[0]
                    if errorstr in output0 and output0[errorstr] == 1:
                        for o in output:
                            del o[errorstr] 
                        raise Exception(output)
                    elif paramsstr in output0 and output0[paramsstr] == 1:
                        for k, v in output0.items():
                            input_shape.set_prop(k, v)
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
        return rs
                    
    def _execute(self, execution_contexts, input_shape, parent_rows, parent_partition_by):
        execution_context =  execution_contexts["db"]
        node_descriptor = self.get_node_descritor()
        input_type = node_descriptor.get_input_type()
        output_partition_by = node_descriptor.get_partition_by()
        use_parent_rows = node_descriptor.get_use_parent_rows()
        node_queries = node_descriptor.get_node_queries()
        paramsstr = "$params"
        errorstr = "$error"        
        breakstr = "$break"

        output = []
        try:            
            execution_context.begin()

            if input_type is not None and input_type == "array":

                length = int(input_shape.get_prop("$length"))
                for i in range(0, length):
                    item_input_shape = input_shape.get_prop("@" + str(i))
                    if node_queries is not None:                
                        for node_query in node_descriptor.get_node_queries():                            
                            query_connection = node_query.get_connection_name()
                            if query_connection is not None:
                                if query_connection in execution_contexts:
                                    sub_output, sub_output_last_inserted_id = execution_contexts[query_connection].execute(node_query, item_input_shape)
                                else:
                                    raise Exception("connection string " + query_connection + " missing")
                            else:
                                sub_output, sub_output_last_inserted_id = execution_context.execute(node_query, item_input_shape)
                                            
                            item_input_shape.set_prop("$params.$last_inserted_id", sub_output_last_inserted_id)

                            if len(sub_output) >= 1:
                                output0 = sub_output[0]
                                if errorstr in output0 and output0[errorstr] == 1:
                                    for o in output:
                                        del o[errorstr] 
                                    raise Exception(output)
                                elif paramsstr in output0 and output0[paramsstr] == 1:
                                    for k, v in output0.items():
                                        item_input_shape.set_prop("$params." + k, v)
                                elif breakstr in output0 and output0[breakstr] == 1:
                                    for o in output:
                                        del o[breakstr] 
                                    break
                                else:
                                    output.extend(sub_output)
            
            elif input_type is None or input_type == "object":
                if node_queries is not None:
                    for node_query in node_descriptor.get_node_queries():
                        query_connection = node_query.get_connection_name()
                        if query_connection is not None:
                            if query_connection in execution_contexts:
                                output, last_inserted_id = execution_contexts[query_connection].execute(node_query, input_shape)
                            else:
                                raise Exception("connection string " + query_connection + " missing")
                        else:
                            output, last_inserted_id = execution_context.execute(node_query, input_shape)

                        input_shape.set_prop("$params.$last_inserted_id", last_inserted_id)

                        if len(output) >= 1:
                            output0 = output[0]
                            if errorstr in output0 and output0[errorstr] == 1:
                                for o in output:
                                    del o[errorstr] 
                                raise Exception(output)
                            elif paramsstr in output0 and output0[paramsstr] == 1:
                                for k, v in output[0].items():
                                    input_shape.set_prop("$params." + k, v)
                            elif breakstr in output0 and output0[breakstr] == 1:
                                for o in output:
                                    del o[breakstr] 
                                break
                
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
                    sub_node_output = sub_node_executor._execute(execution_contexts, sub_node_shape, output, output_partition_by)                    

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

    def get_result(self, execution_contexts, input_shape):
        if "db" not in execution_contexts:
            raise Exception("default connection string name db is missing")
        
        try:
            rs = self._execute(execution_contexts, input_shape, [], None)           
            rs = self.map(rs)
            
            return rs
        except Exception as e:
            #raise e
            return { "errors" : e.args[0] }
    
    def _default_date_time_converter(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    def get_result_json(self, execution_contexts, input_shape):        
        return json.dumps(self.get_result(execution_contexts, input_shape), default= self._default_date_time_converter)

    def set_nodes(self, nodes):
        self._nodes = nodes
    
    def get_nodes(self):
        return self._nodes
    
    def create_input_shape(self, request_body, params, query, path):                
        node_descriptor = self._node_descriptor
        params_shape = Shape({}, None, None, None, None, None)
        if params is not None:
            for k, v in params.items():
                params_shape.set_prop(k, v)

        query_shape = Shape(node_descriptor.get_input_query(), None, None, None, None, None)
        if query is not None:            
            for k, v in query.items():
                query_shape.set_prop(k, v)        

        path_shape = Shape(node_descriptor.get_input_path(), None, None, None, None, None)
        if path is not None:
            for k, v in path.items():
                path_shape.set_prop(k, v)

        input_model = self._node_descriptor.get_input_model()    
        return Shape(input_model, request_body, None, params_shape, query_shape, path_shape)

    def build(self):
        self._node_execution_builder.build(self)

class NodeExecutorBuilder:

    def build(self, node_executor):
        node_descriptor = node_executor.get_node_descritor()

        node_descriptor_nodes = node_descriptor.get_nodes()
        if node_descriptor_nodes is not None:
            nodes = []
            for sub_node_descriptor in node_descriptor_nodes:
                sub_node_executor = NodeExecutor(sub_node_descriptor, self)
                sub_node_executor.build()
                nodes.append(sub_node_executor)

            node_executor.set_nodes(nodes)
