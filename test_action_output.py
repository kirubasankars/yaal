import unittest
from gravity import _get_actions_output, create_context

class FakeExecutionContext:
    
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, descriptor, input_shape, helper):
        if descriptor["content"] == "error":
            return [{ "$http_status_code" : 400 , "$error" : 1, "message" : "message1" }, { "$error" : 1, "message" : "message2", '$http_status_code': 400 }], 0

        if descriptor["content"] == "cookie":
            return [{ "$cookie" : 1, "name" : "name1", "value" : "value" }, { "$cookie" : 1, "name" : "name2", "value" : "value" }], 0

        if descriptor["content"] == "header":
            return [{ "$header" : 1, "name" : "name1", "value" : "value" }, { "$header" : 1, "name" : "name2", "value" : "value" }], 0

        if descriptor["content"] == "params":
            return [{ "$params" : 1, "a" : "1", "b" : "2" }], 1

        if descriptor["content"] == "break":
            return [{ "$break" : 1, "a" : "1", "b" : "2" }], 1

class TestGravity(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def test_descriptor_with_error(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "error"
                }
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        rs, errors = _get_actions_output(descriptor, p, ctx, None)

        self.assertListEqual(errors, [{ "$error" : 1, "message" : "message1", '$http_status_code': 400 }, { "$error" : 1, "message" : "message2", '$http_status_code': 400 }])
        self.assertEqual(ctx.get_prop("$response.status_code"), 400)

    def test_descriptor_with_cookie(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "cookie"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_actions_output(descriptor, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$cookie").get_data(), {'name1': {'$cookie': 1, 'name': 'name1', 'value': 'value'}, 'name2': {'$cookie': 1, 'name': 'name2', 'value': 'value'}})

    def test_descriptor_with_header(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "header"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_actions_output(descriptor, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$header").get_data(), {'name1': {'$header': 1, 'name': 'name1', 'value': 'value'}, 'name2': {'$header': 1, 'name': 'name2', 'value': 'value'}})

    def test_descriptor_with_params(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "params"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_actions_output(descriptor, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$params").get_data(), {'$last_inserted_id': 1, '$params': 1, 'a': '1', 'b': '2'})

    def test_descriptor_with_break(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "break",
                    "connection" : "db"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        rs, errors = _get_actions_output(descriptor, p, ctx, None)

        self.assertListEqual(rs, [{'a': '1', 'b': '2'}])

    
    def test_descriptor_with_connection_missing(self):
        descriptor = {
            "actions" : [
                {
                    "content" : "break",
                    "connection" : "db1"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(descriptor, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        with self.assertRaises(Exception): _get_actions_output(descriptor, p, ctx, None)   

if __name__ == "__main__":
    unittest.main()