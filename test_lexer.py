import unittest
from parser import lexer


class TestLexer(unittest.TestCase):

    def test_simple_sql(self):
        sql = " select  "
        output = lexer(sql)
        self.assertListEqual(output, [{"type":"space","value": " "},{"type":"word","value": "select"},{"type":"space","value": "  "}])

    def test_empty(self):
        self.assertIsNone(lexer(""))
        self.assertIsNone(lexer(None))

    def test_dash(self):
        self.assertListEqual(lexer("--($query.id integer)--"), [{"type":"dash", "value": "--($query.id integer)--"}])

    def test_parameters(self):
        expected = [{"type":"dash", "value": "--($query.id integer)--"},
        {"type": "space", "value": " "}, {"type": "newline", "value": "\n"},
        {"type": "space", "value": " "}, {"type":"word", "value": "select"}, 
        {"type": "space", "value": " "}, {"type": "parameter", "value": "{{$query.id}}"},
        {"type": "space", "value": " "}]

        self.assertListEqual(lexer("""--($query.id integer)-- \n select {{$query.id}} """), expected)

    def test_string(self):
        expected = [ {"type":"word","value": "select"},
        {"type":"space", "value": " "},
        {"type": "string", "value": "'\"dasas'"}]
        self.assertListEqual(lexer("select '\"dasas'"), expected)

    def test_brace(self):
        expected = [{'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, 
        {'type': 'brace', 'value': '(', 'group': 1}, {'type': 'parameter', 'value': '{{a}}'}, 
        {'type': 'space', 'value': ' '}, {'type': 'word', 'value': 'is'}, {'type': 'space', 'value': ' '}, 
        {'type': 'word', 'value': 'null'}, {'type': 'space', 'value': ' '}, 
        {'type': 'word', 'value': 'or'}, {'type': 'space', 'value': ' '},
        {'type': 'brace', 'value': '(', 'group': 2}, {'type': 'word', 'value': '1'}, 
        {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '='}, {'type': 'space', 'value': ' '}, 
        {'type': 'word', 'value': '1'}, {'type': 'brace', 'value': ')', 'group': 2}, 
        {'type': 'brace', 'value': ')', 'group': 1}]

        self.assertListEqual(lexer("select ({{a}} is null or (1 = 1))"), expected)

    def test_sql(self):
        expected = [{'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '1'}, {'type': 'space', 'value': ' '}, {'type': 'dash', 'value': '--sql--'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': 'select'}, {'type': 'space', 'value': ' '}, {'type': 'word', 'value': '1'}]
        self.assertListEqual(lexer("select 1 --sql-- select 1"), expected)

