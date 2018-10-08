import unittest

from yaal import Yaal, create_context


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

    def get_config(self, path):
        return None

    def list_sql(self, path):
        return ["$"]

    def get_routes_config(self, path):
        return None


class TestGravity(unittest.TestCase):
    
    def __init__(self):
        pass

if __name__ == "main":
    unittest.main()

