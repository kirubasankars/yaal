import unittest
from gravity import Gravity, create_context, get_result

class FakeContentReader:

    def get_sql(self, method, path):
        return ""
    
    def get_config(self, method, path):
        pass 

    def list_sql(self, method, path):
        return ["get", "get.data", "get.data.items", "get.data.items.product", "get.paging"]

class TestGravity(unittest.TestCase):
    
    def setUp(self):        
        self._gravity = Gravity("/path", FakeContentReader(), True)        

    def tearDown(self):
        pass
        
    def test_simple_get_descriptor_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1")
        
        self.assertTrue(descriptor_get["name"] == "get")
        self.assertTrue(descriptor_get["method"] == "get")
        
        descritpor_childrens = descriptor_get["childrens"]
        self.assertEqual(len(descritpor_childrens), 2)
        
        descriptor_get_data = descritpor_childrens[0]
        descriptor_get_page = descritpor_childrens[1]
        self.assertTrue(descriptor_get_data["name"] == "data")
        self.assertTrue(descriptor_get_page["name"] == "paging")

        self.assertTrue(descriptor_get_data["method"] == "get.data")
        self.assertTrue(descriptor_get_page["method"] == "get.paging")

        descritpor_childrens = descriptor_get_data["childrens"]    
        self.assertEqual(len(descritpor_childrens), 1)

        descriptor_get_data_items = descritpor_childrens[0]
        self.assertTrue(descriptor_get_data_items["name"] == "items")
        self.assertTrue(descriptor_get_data_items["method"] == "get.data.items")

        descritpor_childrens = descriptor_get_data_items["childrens"]
        self.assertEqual(len(descritpor_childrens), 1)

        descriptor_get_data_items_product = descritpor_childrens[0]       
        self.assertTrue(descriptor_get_data_items_product["name"] == "product")
        self.assertTrue(descriptor_get_data_items_product["method"] == "get.data.items.product")
        
    def test_simple_get_shape_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1")    
        input_shape = create_context(descriptor_get, None, None, None, None, None)
        
        self.assertIsNotNone(input_shape._shapes["data"])
        self.assertIsNotNone(input_shape._shapes["paging"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"]._shapes["product"])
