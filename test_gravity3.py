import unittest
from gravity import Gravity, create_context, get_result_json

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
        return """--(id1, id2)--
select {{id1}}
--query()--
        """
    
    def get_config(self, method, path):
        return None

    def list_sql(self, method, path):        
        return ["get"]

class FakeContentReader1:

    def get_sql(self, method, path):
        return """--(id1 integer, id2 bool)--
select {{id1}}, {{id2}}
--query()--
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
        gravity = Gravity("/path", FakeContentReader(), True)  
        descriptor_get = gravity.create_descriptor("get", "get1")
        
        parameters = descriptor_get["parameters"]
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        queries = descriptor_get["actions"]        
        self.assertIs(type(queries), list)
        self.assertEqual(1, len(queries))
        query0 = queries[0]
        query0_parameters = query0["parameters"]        
        self.assertEqual(1, len(query0_parameters))        
        self.assertEqual("select {{id1}}", query0["content"].lstrip().rstrip())        

    def test_descriptor_with_parameters_queries_check(self):                
        gravity = Gravity("/path", FakeContentReader1(), True)  
        descriptor_get = gravity.create_descriptor("get", "get1")
        
        parameters = descriptor_get["parameters"]
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        queries = descriptor_get["actions"]    
        self.assertIs(type(queries), list)
        self.assertEqual(2, len(queries))
        query0 = queries[0]
        query0_parameters = query0["parameters"]    
        self.assertEqual(2, len(query0_parameters))
        self.assertEqual("integer", query0_parameters[0]["type"])
        self.assertEqual("bool", query0_parameters[1]["type"])

        query1 = queries[1]
        query1_parameters = query1["parameters"]    
        self.assertEqual(1, len(query1_parameters))
        self.assertEqual("bool", query1_parameters[0]["type"])

        s = create_context(descriptor_get, None, None, None, None, None)

if __name__ == "__main__":
    unittest.main()