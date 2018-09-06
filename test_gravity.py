import unittest
from gravity import Gravity, create_context, get_result

class FakeContentReader:

    def get_sql(self, method, path):
        return ""
    
    def get_config(self, method, path):
        pass 

    def list_sql(self, method, path):
        return ["$", "$.data", "$.data.items", "$.data.items.product", "$.paging"]

    def get_routes_config(self, path):
        return None

class TestGravity(unittest.TestCase):
    
    def setUp(self):        
        self._gravity = Gravity("/path", FakeContentReader(), True)        

    def tearDown(self):
        pass
        
    def test_simple_get_trunk_check(self):
        truck = self._gravity.create_trunk("get", "get1")
        
        self.assertTrue(truck["name"] == "$")
        self.assertTrue(truck["method"] == "$")
        
        branches = truck["branches"]
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
        trunk = self._gravity.create_trunk("get", "get1")    
        input_shape = create_context(trunk, "", None, None, None, None, None, None)
        
        self.assertIsNotNone(input_shape._shapes["data"])
        self.assertIsNotNone(input_shape._shapes["paging"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"]._shapes["product"])
