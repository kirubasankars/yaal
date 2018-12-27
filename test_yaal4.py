import unittest

from yaal_parser import parser, lexer
from yaal import _to_lower_keys, _to_lower_keys_deep
from yaal import _order_list_by_dots, _build_trunk_map_by_files, _build_branch


class ContentReader:

    def __init__(self):
        pass

    def get_sql(self, method, path):
        if method == "$":
            return "--($query.id integer)--\n select {{$query.id}}"


class TestGravity(unittest.TestCase):

    def test_simple_object_lower_keys(self):
        obj = {"Name": "k", "Age": 31}
        self.assertDictEqual(_to_lower_keys(obj), {"name": "k", "age": 31})

    def test_complex_object_lower_keys(self):
        obj = {"A": "a", "B": "B", "C": {"D": "d"}}
        self.assertDictEqual(_to_lower_keys(obj), {"a": "a", "b": "B", "c": {"D": "d"}})

    def test_complex_array_object_lower_keys(self):
        obj = {"A": "a", "B": "B", "C": {"D": "d"}, "Array": [{"F": "f"}]}
        self.assertDictEqual(_to_lower_keys(obj), {"a": "a", "b": "B", "c": {"D": "d"}, "array": [{"F": "f"}]})

    def test_object_to_lower_keys_deep(self):
        obj = {"A": "a", "B": "B", "C": {"D": "d"}}
        self.assertDictEqual(_to_lower_keys_deep(obj), {"a": "a", "b": "B", "c": {"d": "d"}})

    def test_complex_array_object_lower_keys_deep(self):
        obj = {"A": "a", "B": "B", "C": {"D": "d"}, "array": [{"F": "f"}]}
        self.assertDictEqual(_to_lower_keys_deep(obj), {"a": "a", "b": "B", "c": {"d": "d"}, "array": [{"f": "f"}]})

    def test_complex1_array_object_lower_keys_deep(self):
        obj = {"A": "a", "B": "B", "C": {"D": "d", "A": "a"}, "array": [{"F": "f", "G": "g"}]}
        self.assertDictEqual(_to_lower_keys_deep(obj),
                             {"a": "a", "b": "B", "c": {"d": "d", "a": "a"}, "array": [{"f": "f", "g": "g"}]})

    def test_order_list_by_dots(self):
        i = ["order.items.product", "order", "order.items"]
        expected = ["order", "order.items", "order.items.product"]
        self.assertListEqual(_order_list_by_dots(i), expected)

    def test_order_list_by_dots_empty(self):
        i = []
        expected = []
        self.assertListEqual(_order_list_by_dots(i), expected)

    def test_order_list_by_dots_single(self):
        i = ["order"]
        expected = ["order"]
        self.assertListEqual(_order_list_by_dots(i), expected)

    def test_trunk_map_by_files(self):
        i = ["order", "order.items", "order.items.product"]
        expected = {"order": {"items": {"product": {}}}}
        self.assertDictEqual(_build_trunk_map_by_files(i), expected)

    def test_trunk_map_by_files1(self):
        i = ["order", "order.items.product"]
        expected = {"order": {"items": {"product": {}}}}
        self.assertDictEqual(_build_trunk_map_by_files(i), expected)

    def test_trunk_map_by_files2(self):
        i = ["order"]
        expected = {"order": {}}
        self.assertDictEqual(_build_trunk_map_by_files(i), expected)

    def test_trunk_map_by_files3(self):
        i = ["order.items.product"]
        expected = {"order": {"items": {"product": {}}}}
        self.assertDictEqual(_build_trunk_map_by_files(i), expected)

    def test_build_branch(self):
        branch, bag, payload_model = {"path": "order/get", "method": "$",
                                      "parameters": {"$query.id": {"name": "$query.id", "type": "integer"}}}, {}, {
                                         "type": "object"}
        output_model, model, content_reader = {}, {"query": {}}, ContentReader()
        expected = {'path': 'order/get', 'method': '$', "twigs": [
            {"content": "\n select {{$query.id}}", "parameters": [{"name": "$query.id", "type": "integer"}]}],
                    "parameters": {"$query.id": {"name": "$query.id", "type": "integer"}}, 'input_type': 'object',
                    'output_type': 'array', 'use_parent_rows': False, 'branches': [
                {'name': 'data', 'method': '$.data', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False},
                {'name': 'paging', 'method': '$.paging', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False}]}
        _build_branch(branch, {"data": {}, "paging": {}}, content_reader, payload_model, output_model, model, bag)
        self.assertDictEqual(branch, expected)

    def test_build_branch1(self):
        branch, bag, payload_model = {"path": "order/get", "method": "$"}, {}, {"type": "object"}
        output_model, model, content_reader = {}, {"query": {}}, ContentReader()
        tree_map = {"data": {}}
        expected = {'path': 'order/get', 'method': '$', "twigs": [
            {"content": "\n select {{$query.id}}", "parameters": [{"name": "$query.id", "type": "integer"}]}],
                    "parameters": {"$query.id": {"name": "$query.id", "type": "integer"}}, 'input_type': 'object',
                    'output_type': 'array', 'use_parent_rows': False, 'branches': [
                {'name': 'data', 'method': '$.data', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False}]}
        _build_branch(branch, tree_map, content_reader, payload_model, output_model, model, bag)
        self.assertDictEqual(branch, expected)

    def test_build_branch2(self):
        branch, bag, payload_model = {"path": "order/get", "method": "$"}, {}, {"type": "object"}
        output_model, model, content_reader = {}, {"query": {}}, ContentReader()
        expected = {'path': 'order/get', 'method': '$', "twigs": [
            {"content": "\n select {{$query.id}}", "parameters": [{"name": "$query.id", "type": "integer"}]}],
                    "parameters": {"$query.id": {"name": "$query.id", "type": "integer"}}, 'input_type': 'object',
                    'output_type': 'array', 'use_parent_rows': False, 'branches': [
                {'name': 'data', 'method': '$.data', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False, "branches": [
                    {"name": "items", "method": "$.data.items", "path": "order/get", "input_type": "object",
                     "output_type": "array", "use_parent_rows": False}]},
                {'name': 'paging', 'method': '$.paging', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False}]}
        _build_branch(branch, {"data": {"items": {}}, "paging": {}}, content_reader, payload_model, output_model, model,
                      bag)
        self.assertDictEqual(branch, expected)

    def test_build_branch3(self):
        branch, bag, payload_model = {"path": "order/get", "method": "$"}, {}, {"type": "object"}
        output_model, model, content_reader = {}, {"query": {}}, ContentReader()
        expected = {'path': 'order/get', 'method': '$', "twigs": [
            {"content": "\n select {{$query.id}}", "parameters": [{"name": "$query.id", "type": "integer"}]}],
                    "parameters": {"$query.id": {"name": "$query.id", "type": "integer"}}, 'input_type': 'object',
                    'output_type': 'array', 'use_parent_rows': False, 'branches': [
                {'name': 'data', 'method': '$.data', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False, "branches": [
                    {"name": "items", "method": "$.data.items", "path": "order/get", "input_type": "object",
                     "output_type": "array", "use_parent_rows": False}]},
                {'name': 'paging', 'method': '$.paging', 'path': 'order/get', 'input_type': 'object',
                 'output_type': 'array', 'use_parent_rows': False}]}
        _build_branch(branch, {"data": {"items": {}}, "paging": {}}, content_reader, payload_model, output_model, model,
                      bag)
        self.assertDictEqual(branch, expected)


if __name__ == "main":
    unittest.main()
