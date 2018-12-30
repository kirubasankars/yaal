import unittest

import yaal
import yaal_flask
from yaal_shape import Shape


class TestYaalContext(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_create_context_returns_empty_shape(self):
        descriptor = {
            "path": "path"
        }

        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertEqual("path", context.get_prop("$params.path"))

    def test_create_context_returns_request_id(self):
        descriptor = {
            "path": ""
        }

        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertIsNotNone(context.get_prop("$request.id"))

    def test_create_context_with_values(self):
        descriptor = {
            "path": ""
        }
        data = {
            "number1": 1,
            "Number2": 2
        }
        context = yaal.create_context(descriptor=descriptor, query=data, path_values=data, header=data, cookie=data,
                                      payload=data)

        self.assertEqual(1, context.get_prop("$query.number1"))
        self.assertEqual(2, context.get_prop("$query.number2"))
        self.assertDictEqual(data, context.get_prop("$query").get_data())

        self.assertEqual(1, context.get_prop("$path.number1"))
        self.assertEqual(2, context.get_prop("$path.number2"))
        self.assertDictEqual(data, context.get_prop("$path").get_data())

        self.assertEqual(1, context.get_prop("$header.number1"))
        self.assertEqual(2, context.get_prop("$header.number2"))
        self.assertDictEqual(data, context.get_prop("$header").get_data())

        self.assertEqual(1, context.get_prop("$cookie.number1"))
        self.assertEqual(2, context.get_prop("$cookie.number2"))
        self.assertDictEqual(data, context.get_prop("$cookie").get_data())

        self.assertDictEqual(data, context.get_data())

        self.assertEqual(Shape, type(context))

    def test_create_context_structure(self):
        descriptor = {
            "path": ""
        }
        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertEqual(Shape, type(context.get_prop("$query")))
        self.assertEqual(Shape, type(context.get_prop("$path")))
        self.assertEqual(Shape, type(context.get_prop("$header")))
        self.assertEqual(Shape, type(context.get_prop("$cookie")))
        self.assertEqual(Shape, type(context.get_prop("$request")))
        self.assertEqual(Shape, type(context.get_prop("$response")))
        self.assertEqual(Shape, type(context.get_prop("$params")))



    def test_create_context_with_validator(self):
        model = {}
        validator = {}
        descriptor = {
            "path": "",
            "model": {
                "query": model,
                "path": model,
                "cookie": model,
                "header": model
            },
            "_validators": {
                "query": validator,
                "path": validator,
                "cookie": validator,
                "header": validator
            }
        }
        context = yaal.create_context(descriptor=descriptor)

        self.assertEqual(model, context.get_prop("$query").get_schema())
        self.assertEqual(validator, context.get_prop("$query").get_validator())

        self.assertEqual(model, context.get_prop("$path").get_schema())
        self.assertEqual(validator, context.get_prop("$path").get_validator())

        self.assertEqual(model, context.get_prop("$cookie").get_schema())
        self.assertEqual(validator, context.get_prop("$cookie").get_validator())

        self.assertEqual(model, context.get_prop("$header").get_schema())
        self.assertEqual(validator, context.get_prop("$header").get_validator())

    def test_expand(self):
        metal = yaal_flask.KeyExpander()

        data = {
            "a.b.number" : 2
        }

        for path in data:
            metal.set_prop(path, data[path])
        print(metal.get_data())