import re
import json


def lex_dash(current, content):
    token = []
    block = True
    token.extend(["-", "-"])
    current = current + 2
    content_length = len(content)
    while True:
        if content_length <= current:
            break

        p = content[current]
        if len(content) > current + 1:
            p1 = content[current + 1]

        if p == "-" and p1 == "-":
            block = None
            token.extend(["-", "-"])
            current = current + 2
            break

        if block:
            token.extend(content[current])
        current = current + 1

    return current, {"type": "dash", "value": "".join(token)}


def lex_curly_braces(current, content):
    token = []
    block = True
    token.extend(["{", "{"])
    current = current + 2
    content_length = len(content)
    while True:
        if content_length <= current:
            break
        p = content[current]
        if len(content) > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""
        if p == "}" and p1 == "}":
            block = None
            token.extend(["}", "}"])
            current = current + 2
            break

        if block:
            token.extend(content[current])
        current = current + 1

    return current, {"type": "parameter", "value": "".join(token)}


def lex_string(current, content, quote):
    token = []
    block = True
    token.extend([quote])
    current = current + 1
    content_length = len(content)
    while True:
        if content_length <= current:
            break
        p = content[current]
        if len(content) > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""
        if p == quote and p1 != quote:
            block = None
            token.extend([quote])
            current = current + 1
            break

        if block:
            token.extend(content[current])
        current = current + 1

    return current, {"type": "string", "value": "".join(token)}


def lex_spaces(current, content):
    start = current
    content_length = len(content)
    while True:
        if content_length <= current:
            break
        p = content[current]
        if p != " ":
            break
        current = current + 1
    return current, {"type": "space", "value": content[start:current]}


def lex_word(current, content):
    singles = "()'\"\n "
    doubles = "{}-"
    token = []
    content_length = len(content)
    while True:
        if content_length <= current:
            break
        p = content[current]
        if len(content) > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""
        if singles.find(p) >= 0 or (doubles.find(p) >= 0 and p == p1):
            break
        else:
            token.append(p)
            current = current + 1
    return current, {"type": "word", "value": "".join(token)}


def lex_braket(current, content):
    return current + 1, {"type": "braket", "value": content[current]}


def lex_newline(current, content):
    return current + 1, {"type": "newline", "value": content[current]}


def lexer(content):
    tokens = []
    current = 0
    content_length = len(content)
    while True:
        if content_length <= current:
            break

        p = content[current]
        if content_length > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""
        if p == "\'" or p == "\"":
            current, t = lex_string(current, content, p)
            tokens.append(t)
        elif p == "-" and p1 == "-":
            current, t = lex_dash(current, content)
            tokens.append(t)
            continue
        elif p == "{" and p1 == "{":
            current, t = lex_curly_braces(current, content)
            tokens.append(t)
            continue
        elif p == " ":
            current, t = lex_spaces(current, content)
            tokens.append(t)
        elif p == "(" or p == ")":
            current, t = lex_braket(current, content)
            tokens.append(t)
        elif p == "\n":
            current, t = lex_newline(current, content)
            tokens.append(t)
        else:
            current, t = lex_word(current, content)
            tokens.append(t)
    return tokens


def parser(tokens):
    ast = {}
    sql_blocks = []
    ast["sql_blocks"] = sql_blocks
    tc = 0
    sql_block = {"stmt": [], "parameters": []}
    parameter_rx = re.compile(r"\s*(?P<name>[\$\_\.A-Za-z0-9]+)(\s+(?P<type>\w+))?\s*")
    sql_rx = re.compile(r"--sql\(\s*(?P<name>\w+)?\s*\)--")
    for token in tokens:
        v = token["value"]
        t = token["type"]
        if t == "parameter":
            parameter_name = v[2:len(v) - 2].lstrip().rstrip()
            sql_block["parameters"].append(parameter_name)
        if t == "dash":
            if v[:3] == "--(" and v[len(v) - 3:] == ")--" and tc == 0:
                v = v[3:len(v) - 3]
                params = v.split(",")
                token["parameters"] = []
                for p in params:
                    m = parameter_rx.search(p)
                    if m:
                        d = m.groupdict()
                        token["parameters"].append({"name": d["name"], "type": d["type"]})
                        d = None
                ast["declaration"] = token
                continue
            else:
                token["type"] = "sql"
                if len(sql_block):
                    sql_blocks.append(sql_block)
                    sql_block = {"stmt": [], "parameters": []}
                    m = sql_rx.search(v)
                    if m:
                        d = m.groupdict()
                        sql_block["connection"] = d["name"]
                    continue
        sql_block["stmt"].append(token)
        tc = tc + 1
    if len(sql_block):
        sql_blocks.append(sql_block)
    return ast


#with open("./serve/api/film/get/$.data.sql", "r") as file:
#    sqlx = file.read()

#output = parser(lexer(sqlx))
# print(json.dumps(output, indent=4))
# print("".join(lexer_output) == content)
#print(output)
