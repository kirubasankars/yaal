import unittest
from gravity import Gravity
from gravity import GravityConfiguration
from gravity import ExecutionContext
from nodedescriptor import NodeDescriptorParameter

class FakeExecutionContext:
    
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_executor, input_shape):        
        return [input_shape._data], 0

class FakeContentReader:

    def get_sql(self, method, path):
        return """--params(id1, id2)--
select {{id1}}
--query()()--
        """
    
    def get_config(self, method, path):
        return None

    def list_sql(self, method, path):        
        return ["get"]

class FakeContentReader1:

    def get_sql(self, method, path):
        return """--params(id1 integer, id2 bool)--
select {{id1}}, {{id2}}
--query()()--
select {{id2}}
        """
    
    def get_config(self, method, path):
        return None

    def list_sql(self, method, path):        
        return ["get"]


class TestGravity(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def test_descriptor_with_parameters_query_check(self):
        gravity_config = GravityConfiguration("/path")
        gravity = Gravity(gravity_config, FakeContentReader())  
        descriptor_get = gravity.create_descriptor("get", "get1", True)
        
        parameters = descriptor_get.get_parameters()
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        queries = descriptor_get.get_node_queries()        
        self.assertIs(type(queries), list)
        self.assertEqual(1, len(queries))
        query0 = queries[0]
        query0_parameters = query0.get_parameters()        
        self.assertEqual(1, len(query0_parameters))        
        self.assertEqual("select {{id1}}", query0.get_content().lstrip().rstrip())        

    def test_descriptor_with_parameters_queries_check(self):        
        gravity_config = GravityConfiguration("/path")
        gravity = Gravity(gravity_config, FakeContentReader1())  
        descriptor_get = gravity.create_descriptor("get", "get1", True)
        
        parameters = descriptor_get.get_parameters()
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        queries = descriptor_get.get_node_queries()        
        self.assertIs(type(queries), list)
        self.assertEqual(2, len(queries))
        query0 = queries[0]
        query0_parameters = query0.get_parameters()    
        self.assertEqual(2, len(query0_parameters))
        self.assertEqual("integer", query0_parameters[0].get_type())
        self.assertEqual("bool", query0_parameters[1].get_type())

        query1 = queries[1]
        query1_parameters = query1.get_parameters()    
        self.assertEqual(1, len(query1_parameters))
        self.assertEqual("bool", query1_parameters[0].get_type())

        e = descriptor_get.create_executor()        
        s = e.create_input_shape(None)

if __name__ == "__main__":
    unittest.main()