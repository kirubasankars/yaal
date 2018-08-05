import unittest
from gravity import Gravity
from gravity import GravityConfiguration

class FakeExecutionContext:
    
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, node_executor, input_shape):        
       return []

class FakeContentReader:

    def get_sql(self, method, path):
        return ""
    
    def get_config(self, method, path):
        pass 

    def list_sql(self, method, path):
        return ["get", "get.data", "get.data.items", "get.data.items.product", "get.paging"]

class TestGravity(unittest.TestCase):
    
    def setUp(self):
        self._gravity_config = GravityConfiguration("/path")
        self._gravity = Gravity(self._gravity_config, FakeExecutionContext(), FakeContentReader())        

    def tearDown(self):
        pass
        
    def test_simple_get_descriptor_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1", True)
        
        self.assertTrue(descriptor_get.get_name() == "get")
        self.assertTrue(descriptor_get.get_method() == "get")
        
        descritpor_nodes = descriptor_get.get_nodes()
        self.assertEqual(len(descritpor_nodes), 2)
        
        descriptor_get_data = descritpor_nodes[0]
        descriptor_get_page = descritpor_nodes[1]
        self.assertTrue(descriptor_get_data.get_name() == "data")
        self.assertTrue(descriptor_get_page.get_name() == "paging")

        self.assertTrue(descriptor_get_data.get_method() == "get.data")
        self.assertTrue(descriptor_get_page.get_method() == "get.paging")

        descritpor_nodes = descriptor_get_data.get_nodes()    
        self.assertEqual(len(descritpor_nodes), 1)

        descriptor_get_data_items = descritpor_nodes[0]
        self.assertTrue(descriptor_get_data_items.get_name() == "items")
        self.assertTrue(descriptor_get_data_items.get_method() == "get.data.items")

        descritpor_nodes = descriptor_get_data_items.get_nodes()
        self.assertEqual(len(descritpor_nodes), 1)

        descriptor_get_data_items_product = descritpor_nodes[0]       
        self.assertTrue(descriptor_get_data_items_product.get_name() == "product")
        self.assertTrue(descriptor_get_data_items_product.get_method() == "get.data.items.product")
        
    def test_simple_get_executor_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1", True)
        executor_get = descriptor_get.create_executor(FakeExecutionContext())
        
        self.assertTrue(executor_get.get_node_descritor().get_name() == "get")

        executor_nodes = executor_get.get_nodes()
        self.assertEqual(len(executor_nodes), 2)
        
        executor_get_data = executor_nodes[0]
        executor_get_page = executor_nodes[1]

        self.assertTrue(executor_get_data.get_node_descritor().get_name() == "data")
        self.assertTrue(executor_get_page.get_node_descritor().get_name() == "paging")
        
        executor_nodes = executor_get_data.get_nodes()    
        self.assertEqual(len(executor_nodes), 1)
        
        executor_get_data_items = executor_nodes[0]
        self.assertTrue(executor_get_data_items.get_node_descritor().get_name() == "items")
        
        executor_nodes = executor_get_data_items.get_nodes()
        self.assertEqual(len(executor_nodes), 1)

        executor_get_data_items_product = executor_nodes[0]
        self.assertTrue(executor_get_data_items_product.get_node_descritor().get_name() == "product")

    def test_simple_get_shape_check(self):
        descriptor_get = self._gravity.create_descriptor("get", "get1", True)
        executor_get = descriptor_get.create_executor(FakeExecutionContext())
        input_shape = executor_get.create_input_shape(None)
        
        self.assertIsNotNone(input_shape._shapes["data"])
        self.assertIsNotNone(input_shape._shapes["paging"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"])
        self.assertIsNotNone(input_shape._shapes["data"]._shapes["items"]._shapes["product"])
         

if __name__ == "__main__":
    unittest.main()