import unittest

from jsonschema import FormatChecker, Draft4Validator

from yaal_shape import Shape


class TestShape(unittest.TestCase):

    def test_shape_no_reserved_keywords(self):
        with self.assertRaises(ValueError): Shape(data={"$parent": "1"})
        with self.assertRaises(ValueError): Shape(data={"$length": "1"})

    def test_shape_expected_types(self):
        with self.assertRaises(TypeError): Shape(schema={"type": "object"}, data=[{"number": 1}])
        with self.assertRaises(TypeError): Shape(schema={"type": "array"}, data={"number": 1})

        with self.assertRaises(TypeError): Shape(schema={"type": "object"}, data=[1])

        Shape(schema={"type": "array"}, data=None)
        Shape(schema={"type": "object"}, data=None)

    def test_shape_lower_case_keys_access(self):
        schema = {
            "type": "object",
            "properties": {
                "obj": {
                    "type": "object"
                },
                "list": {
                    "type": "array"
                }
            }
        }

        data = {
            "Number": 1,
            "obj": {
                "nuMber": 1
            },
            "lIst": [
                {
                    "numbeR": 1
                },
                {
                    "numbeR": 2
                }
            ],
            "obj1": {
                "numBer": 1
            },
            "array1": [1, 2]
        }

        s1 = Shape(schema=schema, data=data)
        self.assertEqual(1, s1.get_prop("number"))
        self.assertEqual(1, s1.get_prop("obj.number"))
        self.assertDictEqual({"numBer": 1}, s1.get_prop("obj1"))

        self.assertEqual(1, s1.get_prop("list.$0.number"))

        with self.assertRaises(KeyError): self.assertDictEqual(1, s1.get_prop("list.0"))
        with self.assertRaises(KeyError): self.assertDictEqual(1, s1.get_prop("list.0.number"))

        s1 = Shape(data=data)
        self.assertEqual(1, s1.get_prop("number"))

    def test_shape_array_functions(self):
        schema = {
            "type": "object",
            "properties": {
                "list": {
                    "type": "array"
                }
            }
        }

        data = {
            "lIst": [
                {
                    "numbeR": 1
                },
                {
                    "numbeR": 2
                }
            ]
        }

        s1 = Shape(schema, data)
        self.assertEqual(1, s1.get_prop("list.$0.number"))
        self.assertEqual(2, s1.get_prop("list.$length"))
        self.assertEqual(1, s1.get_prop("list.$1.$index"))
        self.assertEqual(data["lIst"][1], s1.get_prop("list.$1").get_data())
        with self.assertRaises(KeyError): self.assertDictEqual(1, s1.get_prop("list.0.number"))

    def test_shape_no_access_to_non_defined_properties(self):
        schema = {
            "type": "object",
            "properties": {
                "obj": {
                    "type": "object"
                }
            }
        }

        data = {
            "Number": 1,
            "obj": {
                "nuMber": 1
            },
            "obj1": {
                "numBer": 1
            },
            "array1": [1, 2]
        }

        s1 = Shape(schema, data)
        self.assertDictEqual({"numBer": 1}, s1.get_prop("obj1"))
        with self.assertRaises(KeyError): self.assertDictEqual({"numBer": 1}, s1.get_prop("obj1.number"))

    def test_shape_with_parent(self):
        with self.assertRaises(TypeError): self.assertDictEqual(Shape(parent_shape={}))

        parent = Shape()
        s1 = Shape(parent_shape=parent)
        self.assertEqual(parent, s1.get_prop("$parent"))

    def test_shape_with_extras(self):
        extras = {
            "$test": Shape(data={"number": 1})
        }
        s1 = Shape(extras=extras)
        self.assertEqual(extras["$test"], s1.get_prop("$test"))
        self.assertEqual(1, s1.get_prop("$test.number"))

    def test_shape_with_default_value(self):
        schema = {
            "type": "object",
            "properties": {
                "number": {
                    "default": 1
                }
            }
        }
        s1 = Shape(schema=schema)
        self.assertEqual(1, s1.get_prop("number"))

        data = {
            "number": 2
        }
        s1 = Shape(schema=schema, data=data)
        self.assertEqual(2, s1.get_prop("number"))

    def test_shape_with_allocation(self):
        schema = {
            "type": "object",
            "properties": {
                "obj1": {
                    "type": "object",
                    "properties": {
                        "list": {
                            "type": "array"
                        },
                        "obj2": {
                            "type": "object",
                            "properties": {
                                "obj3": {
                                    "type": "object"
                                },
                                "obj4": {
                                    "properties": {

                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        s1 = Shape(schema=schema)
        self.assertEqual(Shape, type(s1.get_prop("obj1")))
        self.assertEqual(Shape, type(s1.get_prop("obj1.list")))
        self.assertEqual(Shape, type(s1.get_prop("obj1.obj2.obj3")))
        self.assertEqual(None, s1.get_prop("obj1.obj2.obj4"))

    def test_shape_with_allocation1(self):
        schema = {
            "type": "object",
            "properties": {
                "obj1": {
                    "type": "object"
                },
                "obj2": {
                    # TODO: validation require to check type property in schema definition,
                    # can be implemented with json schema validation
                    "properties": {
                        "obj3": {
                            "type": "object"
                        }
                    }
                }
            }
        }

        data = {
            "obj1": {
                "number": 1
            },
            "obj2": {
                "number": 1,
                "obj3": {
                    "number": 1
                }
            }
        }

        s1 = Shape(schema=schema, data=data)
        self.assertEqual(Shape, type(s1.get_prop("obj1")))
        self.assertEqual(1, s1.get_prop("obj1.number"))
        self.assertEqual(dict, type(s1.get_prop("obj2")))
        with self.assertRaises(KeyError): self.assertEqual(None, type(s1.get_prop("obj2.obj3")))

    def test_shape_with_set_property(self):
        schema = {
            "type": "object",
            "properties": {
                "obj1": {
                    "type": "object",
                    "properties": {
                        "number": {
                            "format": "integer"
                        },
                        "obj2": {
                            "type": "object",
                            "properties": {
                                "number": {
                                    "type": "integer"
                                }
                            }
                        }
                    }
                },
                "list": {
                    "type": "array",
                    "properties": {
                        "number": {
                            "type": "integer"
                        }
                    }
                }
            }
        }

        data = {
            "obj1": {
                "number": 1
            },
            "list": [
                {
                    "number": 1
                }
            ]
        }

        s1 = Shape(schema=schema, data=data)

        s1.set_prop("obj1.number", 1)
        self.assertEqual(1, s1.get_prop("obj1.number"))

        s1.set_prop("obj1.obj2.number", 1)
        self.assertEqual(1, s1.get_prop("obj1.obj2.number"))

        s1.set_prop("obj1.obj2.number", "1")
        self.assertEqual(1, s1.get_prop("obj1.obj2.number"))

        with self.assertRaises(ValueError): s1.set_prop("obj1.obj2.number", "a")

        s1.set_prop("list.$0.number", 1)
        self.assertEqual(1, s1.get_prop("list.$0.number"))

        s1.set_prop("list.$0.number", "1")
        self.assertEqual(1, s1.get_prop("list.$0.number"))
        with self.assertRaises(ValueError): s1.set_prop("list.$0.number", "a")

        with self.assertRaises(IndexError): s1.set_prop("list.$1.number", "1")

        s1.set_prop("list.$0.Number", 2)
        self.assertEqual(2, s1.get_prop("list.$0.number"))

        s1.set_prop("obj1.number", None)
        self.assertEqual(None, s1.get_prop("obj1.number"))

    def test_shape_set_property1(self):
        data = {
            "number": 1,
            "Number": 2
        }

        s1 = Shape(data=data)
        self.assertDictEqual(data, s1.get_data())

        s1.set_prop("Number", 1)
        self.assertDictEqual({'Number': 1}, s1.get_data())



    def test_shape_with_validation(self):
        schema = {
            "type": "object",
            "properties": {
                "number": {
                    "type": "integer"
                },
                "obj": {
                    "type": "object",
                    "properties": {
                        "number": {
                            "type": "integer"
                        }
                    }
                }
            }
        }

        validator = Draft4Validator(schema=schema, format_checker=FormatChecker())
        data = {
            "number": "a",
            "obj": {
                "number": "c"
            }
        }

        s1 = Shape(schema=schema, data=data, validator=validator)
        self.assertListEqual([{'message': "'a' is not of type 'integer'"}, {'message': "'c' is not of type 'integer'"}],
                             s1.validate())

    def test_shape_extras_with_validation(self):
        schema = {
            "type": "object",
            "properties": {
                "number": {
                    "type": "integer"
                }
            }
        }

        validator = Draft4Validator(schema=schema, format_checker=FormatChecker())
        data = {
            "number": "a"
        }
        e = Shape(schema=schema, data=data, validator=validator)
        extras = {"e": e}
        s1 = Shape(schema=schema, data=data, validator=validator, extras=extras)

        self.assertListEqual([{'message': "'a' is not of type 'integer'", 'name': 'e'}, {'message': "'a' is not of type 'integer'"}],
                             s1.validate(True))
