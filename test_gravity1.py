import unittest
from gravity import Gravity, create_context, get_result

class FakeContentReader:

    def get_sql(self, method, path):
        return ""
    
    def get_config(self, path):
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

    def list_sql(self, path):
        return ["$"]

    def get_routes_config(self, path):
        return None

class TestGravity(unittest.TestCase):
    
    def setUp(self):
        self._gravity = Gravity("/path", FakeContentReader(), True)        

    def tearDown(self):
        pass
        
    def test_simple_get_trunk_check(self):
        trunk = self._gravity.create_descriptor("get1/get")
        
        self.assertTrue(trunk["name"] == "$")
        self.assertTrue(trunk["method"] == "$")
        
        branches = trunk["branches"]
        self.assertEqual(len(branches), 2)
        
        branch_data = branches[0]
        branch_page = branches[1]
        self.assertTrue(branch_data["name"] == "data")
        self.assertTrue(branch_page["name"] == "paging")

        self.assertTrue(branch_data["method"] == "$.data")
        self.assertTrue(branch_page["method"] == "$.paging")

        branches = branch_data["branches"]    
        self.assertEqual(len(branches), 1)

        branch_data_items = branches[0]
        self.assertTrue(branch_data_items["name"] == "items")
        self.assertTrue(branch_data_items["method"] == "$.data.items")

        branches = branch_data_items["branches"]
        self.assertEqual(len(branches), 1)

        branch_data_items_product = branches[0]       
        self.assertTrue(branch_data_items_product["name"] == "product")
        self.assertTrue(branch_data_items_product["method"] == "$.data.items.product")
        
    def test_simple_get_shape_check(self):
        branch = self._gravity.create_descriptor("get1/get")        
        input_shape = create_context(branch, "", None, None, None, None, None, None)
        
        self.assertIsNotNone(input_shape._shapes["data"])
        self.assertIsNotNone(input_shape._shapes["paging"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"]._shapes["product"])

if __name__ == "__main__":
    unittest.main()