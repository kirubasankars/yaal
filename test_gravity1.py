import unittest
from gravity import Gravity, create_context, get_result

class FakeContentReader:

    def get_sql(self, method, path):
        return ""
    
    def get_config(self, method, path):
        return {
            "output.model" : {
                "type" : "object",
                "properties" : {
                    "data" : {    
                        "type" : "array",                
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
                    },
                    "paging" : {
                        "type" : "object"                        
                    }
                }
            }
        }

    def list_sql(self, method, path):
        
        return ["get"]

    def get_routes_config(self, path):
        return None

class TestGravity(unittest.TestCase):
    
    def setUp(self):
        self._gravity = Gravity("/path", FakeContentReader(), True)        

    def tearDown(self):
        pass
        
    def test_simple_get_descriptor_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1")
        
        self.assertTrue(descriptor_get["name"] == "get")
        self.assertTrue(descriptor_get["method"] == "get")
        
        descritpor_nodes = descriptor_get["branches"]
        self.assertEqual(len(descritpor_nodes), 2)
        
        descriptor_get_data = descritpor_nodes[0]
        descriptor_get_page = descritpor_nodes[1]
        self.assertTrue(descriptor_get_data["name"] == "data")
        self.assertTrue(descriptor_get_page["name"] == "paging")

        self.assertTrue(descriptor_get_data["method"] == "get.data")
        self.assertTrue(descriptor_get_page["method"] == "get.paging")

        descritpor_nodes = descriptor_get_data["branches"]    
        self.assertEqual(len(descritpor_nodes), 1)

        descriptor_get_data_items = descritpor_nodes[0]
        self.assertTrue(descriptor_get_data_items["name"] == "items")
        self.assertTrue(descriptor_get_data_items["method"] == "get.data.items")

        descritpor_nodes = descriptor_get_data_items["branches"]
        self.assertEqual(len(descritpor_nodes), 1)

        descriptor_get_data_items_product = descritpor_nodes[0]       
        self.assertTrue(descriptor_get_data_items_product["name"] == "product")
        self.assertTrue(descriptor_get_data_items_product["method"] == "get.data.items.product")
        
    def test_simple_get_shape_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1")        
        input_shape = create_context(descriptor_get, "", None, None, None, None, None, None)
        
        self.assertIsNotNone(input_shape._shapes["data"])
        self.assertIsNotNone(input_shape._shapes["paging"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"]._shapes["product"])

if __name__ == "__main__":
    unittest.main()