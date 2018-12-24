import unittest
from yaal_parser import lexer, parser


class TestParser(unittest.TestCase):

    def test_simple_sql(self):
        sql = " select  "
        output = parser(lexer(sql))
        expected = {"sql_stmts": [{ "content": [{"type":"space","value": " "},{"type":"word","value": "select"},{"type":"space","value": "  "}], "parameters": []}]}
        self.assertDictEqual(output, expected)

    def test_empty(self):
        self.assertIsNone(parser(None))

    def test_dash(self):
        self.assertDictEqual(parser(lexer("--($query.id integer)--")),{'parameters': {'$query.id': {'name': '$query.id', 'type': 'integer'}}})

    def test_parameters(self):
        expected = {'sql_stmts': [{'content': [{'type': 'space', 'value': ' '}, {'type': 'newline', 'value': '\n'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'parameter', 'value': '{{$query.id}}', 'name': '$query.id'}, {'type': 'space', 'value': ' '}], 'parameters': [{'name': '$query.id', 'type': 'integer'}]}], 'parameters': {'$query.id': {'name': '$query.id', 'type': 'integer'}}}
        self.assertDictEqual(parser(lexer("""--($query.id integer)-- \n select {{$query.id}} """)), expected)

    def test_string(self):
        expected = {'sql_stmts': [{'content': [{'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'string', 'value': '"\'dasas\'"'}], 'parameters': []}]}
        self.assertDictEqual(parser(lexer("select \"'dasas'\"")), expected)

    def test_brace(self):
        expected = {'sql_stmts': [{'content': [{'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'brace', 'value': '(', 'group': 1, 'nullable_parameter': 'a'}, {'type': 'parameter', 'name': 'a', 'value': '{{a}} is null', 'nullable': True}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': 'or'}, {'type': 'space', 'value': ' '}, {'type': 'brace', 'value': '(', 'group': 2}, {'type': 'word', 'value': '1'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '='}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '1'}, {'type': 'brace', 'value': ')', 'group': 2}, {'type': 'brace', 'value': ')', 'group': 1}], 'parameters': [{"name": 'a'}], 'nullable': ['a']}]}
        self.assertDictEqual(parser(lexer("select ({{a}} is null or (1 = 1))")), expected)

    def test_sql(self):
        expected = {'sql_stmts': [{'content': [{'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '1'}, {'type': 'space', 'value': ' '}], 'parameters': []}, {'content': [{'type': 'space', 'value': ' '}, {'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '1'}], 'parameters': []}]}
        self.assertDictEqual(parser(lexer("select 1 --sql-- select 1")), expected)

