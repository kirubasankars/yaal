import unittest
from gravity import _get_leafs_output, create_context

class FakeExecutionContext:
    
    def begin(self):
        pass

    def end(self):
        pass
    
    def error(self):
        pass

    def execute(self, trunk, input_shape, helper):
        if trunk["content"] == "error":
            return [{ "$http_status_code" : 400 , "$error" : 1, "message" : "message1" }, { "$error" : 1, "message" : "message2", '$http_status_code': 400 }], 0

        if trunk["content"] == "cookie":
            return [{ "$cookie" : 1, "name" : "name1", "value" : "value" }, { "$cookie" : 1, "name" : "name2", "value" : "value" }], 0

        if trunk["content"] == "header":
            return [{ "$header" : 1, "name" : "name1", "value" : "value" }, { "$header" : 1, "name" : "name2", "value" : "value" }], 0

        if trunk["content"] == "params":
            return [{ "$params" : 1, "a" : "1", "b" : "2" }], 1

        if trunk["content"] == "break":
            return [{ "$break" : 1, "a" : "1", "b" : "2" }], 1

class TestGravity(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def test_trunk_with_error(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "error"
                }
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        rs, errors = _get_leafs_output(trunk, p, ctx, None)

        self.assertListEqual(errors, [{ "$error" : 1, "message" : "message1", '$http_status_code': 400 }, { "$error" : 1, "message" : "message2", '$http_status_code': 400 }])
        self.assertEqual(ctx.get_prop("$response.status_code"), 400)

    def test_trunk_with_cookie(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "cookie"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_leafs_output(trunk, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$cookie").get_data(), {'name1': {'$cookie': 1, 'name': 'name1', 'value': 'value'}, 'name2': {'$cookie': 1, 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_header(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "header"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_leafs_output(trunk, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$response.$header").get_data(), {'name1': {'$header': 1, 'name': 'name1', 'value': 'value'}, 'name2': {'$header': 1, 'name': 'name2', 'value': 'value'}})

    def test_trunk_with_params(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "params"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        _get_leafs_output(trunk, p, ctx, None)

        self.assertDictEqual(ctx.get_prop("$params").get_data(), {'$last_inserted_id': 1, '$params': 1, 'a': '1', 'b': '2'})

    def test_trunk_with_break(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "break",
                    "connection" : "db"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        rs, errors = _get_leafs_output(trunk, p, ctx, None)

        self.assertListEqual(rs, [{'a': '1', 'b': '2'}])

    def test_trunk_with_connection_missing(self):
        trunk = {
            "leafs" : [
                {
                    "content" : "break",
                    "connection" : "db1"
                }                
            ],
            "input_type" : "object",
            "output_type" : "array",
            "_validators" : None
        }
        
        ctx = create_context(trunk, "user", None, None, None, None, None, None)

        p = {
            "db" : FakeExecutionContext()
        }

        with self.assertRaises(Exception): _get_leafs_output(trunk, p, ctx, None)   

if __name__ == "__main__":
    unittest.main()