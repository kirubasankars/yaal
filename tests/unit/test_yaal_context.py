# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import unittest

import yaal
from yaal_shape import Shape


class TestYaalContext(unittest.TestCase):

    def test_create_context_returns_empty_shape(self):
        descriptor = {
            "path": "path"
        }

        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertEqual("path", context.get_prop("$params.path"))

    def test_create_context_returns_run_id(self):
        descriptor = {
            "path": ""
        }

        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertIsNotNone(context.get_prop("$params.$run_id"))

    def test_create_context_with_values(self):
        descriptor = {
            "path": ""
        }
        data = {
            "number1": 1,
            "Number2": 2
        }
        context = yaal.create_context(descriptor=descriptor, args=data, payload=data)

        self.assertEqual(1, context.get_prop("$args.number1"))
        self.assertEqual(2, context.get_prop("$args.number2"))
        self.assertDictEqual(data, context.get_prop("$args").get_data())

        self.assertDictEqual(data, context.get_data())

        self.assertEqual(Shape, type(context))

    def test_create_context_structure(self):
        descriptor = {
            "path": ""
        }
        context = yaal.create_context(descriptor=descriptor)
        self.assertEqual(Shape, type(context))
        self.assertEqual(Shape, type(context.get_prop("$args")))
        self.assertEqual(Shape, type(context.get_prop("$params")))

    def test_create_context_with_validator(self):
        model = {}
        validator = {}
        descriptor = {
            "path": "",
            "model": {
                "args": model,
                "payload": model,
            },
            "_validators": {
                "args": validator,
                "payload": validator,
            }
        }
        context = yaal.create_context(descriptor=descriptor)

        self.assertEqual(model, context.get_prop("$args").get_schema())
        self.assertEqual(validator, context.get_prop("$args").get_validator())
        self.assertEqual(model, context.get_schema())
        self.assertEqual(validator, context.get_validator())
