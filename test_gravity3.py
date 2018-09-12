import unittest

from gravity import Gravity, create_context


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


class FakeContentReader1:

    def get_sql(self, method, path):
        return """--(id1 integer, id2 bool)--
select {{id1}}, {{id2}}
--query()--
select {{id2}}
        """

    def get_config(self, path):
        return None

    def list_sql(self, path):
        return ["$"]

    def get_routes_config(self, path):
        return None


class TestGravity(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_trunk_with_parameters_query_check(self):
        gravity = Gravity("/path", FakeContentReader(), True)
        trunk = gravity.create_descriptor("get1/get")

        parameters = trunk["parameters"]
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        leafs = trunk["leafs"]
        self.assertIs(type(leafs), list)
        self.assertEqual(1, len(leafs))
        leaf0 = leafs[0]
        leaf0_parameters = leaf0["parameters"]
        self.assertEqual(1, len(leaf0_parameters))
        self.assertEqual("select {{id1}}", leaf0["content"].lstrip().rstrip())

    def test_trunk_with_parameters_queries_check(self):
        gravity = Gravity("/path", FakeContentReader1(), True)
        trunk = gravity.create_descriptor("get1/get")

        parameters = trunk["parameters"]
        self.assertIn("id1", parameters)
        self.assertIn("id2", parameters)
        self.assertIs(type(parameters), dict)

        leafs = trunk["leafs"]
        self.assertIs(type(leafs), list)
        self.assertEqual(2, len(leafs))
        leaf0 = leafs[0]
        leaf0_parameters = leaf0["parameters"]
        self.assertEqual(2, len(leaf0_parameters))
        self.assertEqual("integer", leaf0_parameters[0]["type"])
        self.assertEqual("bool", leaf0_parameters[1]["type"])

        leaf1 = leafs[1]
        leaf1_parameters = leaf1["parameters"]
        self.assertEqual(1, len(leaf1_parameters))
        self.assertEqual("bool", leaf1_parameters[0]["type"])

        s = create_context(trunk, "", None, None, None, None, None, None)


if __name__ == "__main__":
    unittest.main()
