import unittest
from gravity import Gravity,create_context, get_result

class FakeExecutionContext:
    
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_executor, input_shape, helper):        
        return [input_shape._data], 0

class FakeContentReader:

    def get_sql(self, method, path):
        return "INSERT"
    
    def get_config(self, method, path):
        return {
            "input.model" : {
                "body" : {
                    "type" : "object",
                    "properties" : {
                        "items": {
                            "type" : "array",
                            "properties" : {                            
                                "product" : {
                                    "type" : "object"
                                }
                            }
                        }
                    }
                }
            }
        }

    def list_sql(self, method, path):        
        return ["post", "post.items"]
    
    def get_routes_config(self, path):
        return None

class TestGravity(unittest.TestCase):
    
    def setUp(self):        
        self._gravity = Gravity("/path", FakeContentReader(), True)        

    def tearDown(self):
        pass
        
    def test_simple_post_descriptor_check(self):
        descriptor_post = self._gravity.create_descriptor("post", "post1")
        
        self.assertTrue(descriptor_post["name"] == "post")
        self.assertTrue(descriptor_post["method"] == "post")
        
        descritpor_nodes = descriptor_post["branches"]
        self.assertEqual(len(descritpor_nodes), 1)
                
        descriptor_post_data_items = descritpor_nodes[0]
        self.assertTrue(descriptor_post_data_items["name"] == "items")
        self.assertTrue(descriptor_post_data_items["method"] == "post.items")

        descritpor_nodes = descriptor_post_data_items["branches"]
        self.assertEqual(descritpor_nodes, None)

    def test_simple_get_shape_check(self):        
        descriptor_post = self._gravity.create_descriptor("post", "post1")        
        input_shape = create_context(descriptor_post, "", None, None, None, None, None, None)
        
        self.assertIsNotNone(input_shape._shapes["items"])        
        self.assertEqual(len(input_shape._shapes["items"]._shapes),0)

    def get_data_provider(self, name):
        return FakeExecutionContext()

    def test_run(self):
        descriptor_post = self._gravity.create_descriptor("post", "post1")        
        d = {"items":[{"a":1}, {"b":1}]}
        input_shape = create_context(descriptor_post, "", d, None, None, None, None, None)            
        rs = get_result(descriptor_post, self.get_data_provider, input_shape)

        self.assertListEqual(rs, [d])

if __name__ == "__main__":
    unittest.main()