import unittest

from gravity import _build_trunk_map_by_files, _order_list_by_dots


class TestBranchMap(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_branch_map_order_by_dots(self):
        name_list = _order_list_by_dots(["$", "$.paging", "$.data.items.product", "$.data", "$.data.items"])
        self.assertListEqual(name_list, ["$", "$.paging", "$.data", "$.data.items", "$.data.items.product"])

    def test_branch_map_order_by_dots1(self):
        name_list = _order_list_by_dots(["$", "$.Paging", "$.data.items.product", "$.data", "$.data.items"])
        self.assertListEqual(name_list, ["$", "$.paging", "$.data", "$.data.items", "$.data.items.product"])

    def test_branch_map_nesting(self):
        name_list = ["$", "$.paging", "$.data.items.product", "$.data", "$.data.items"]
        name_list = _build_trunk_map_by_files(name_list)
        self.assertDictEqual(name_list, {"$": {"paging": {}, "data": {"items": {"product": {}}}}})


if __name__ == "__main__":
    unittest.main()
