import unittest

from yaal import _to_lower_keys, _to_lower_keys_deep

class TestGravity(unittest.TestCase):
    
    def test_simple_object_lower_keys(self):
       obj = {"Name": "kiruba", "Age": 31}
       self.assertDictEqual(_to_lower_keys(obj), {"name": "kiruba", "age": 31})

    def test_complex_object_lower_keys(self):
       obj = {"A": "a", "B": "B", "C": {"D":"d"}}
       self.assertDictEqual(_to_lower_keys(obj), {"a":"a","b":"B","c":{"D":"d"}})

    def test_complex_array_object_lower_keys(self):
       obj = {"A": "a", "B": "B", "C": {"D":"d"},"Array":[{"F":"f"}]}
       self.assertDictEqual(_to_lower_keys(obj), {"a": "a", "b": "B", "c": {"D":"d"},"array":[{"F":"f"}]})

    def test_object_to_lower_keys_deep(self):
       obj = {"A": "a", "B": "B", "C": {"D":"d"}}
       self.assertDictEqual(_to_lower_keys_deep(obj), {"a":"a","b":"B","c":{"d":"d"}})

    def test_complex_array_object_lower_keys_deep(self):
       obj = {"A": "a", "B": "B", "C": {"D":"d"},"array":[{"F":"f"}]}
       self.assertDictEqual(_to_lower_keys_deep(obj), {"a": "a", "b": "B", "c": {"d":"d"},"array":[{"f":"f"}]})


if __name__ == "main":
    unittest.main()

