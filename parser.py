import re
import json


def lex_dash(current, content):
    token = []
    token.extend(["-", "-"])
    current = current + 2
    content_length = len(content)
    while True:
        if content_length <= current:
            break

        p = content[current]
        if content_length > current + 1:
            p1 = content[current + 1]

        if p == "-" and p1 == "-":
            token.extend(["-", "-"])
            current = current + 2
            break

        token.extend(content[current])
        current = current + 1

    return current, {"type": "dash", "value": "".join(token)}


def lex_curly_braces(current, content):
    token = []
    token.extend(["{", "{"])
    current = current + 2
    content_length = len(content)
    while True:
        if content_length <= current:
            break

        p = content[current]

        if content_length > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""

        if p == "}" and p1 == "}":
            token.extend(["}", "}"])
            current = current + 2
            break

        token.extend(content[current])
        current = current + 1

    return current, {"type": "parameter", "value": "".join(token)}


def lex_string(current, content, quote):
    token = []
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
            token.extend([quote])
            current = current + 1
            break

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


def lex_brace(current, content):
    return current + 1, {"type": "brace", "value": content[current]}


def lex_newline(current, content):
    return current + 1, {"type": "newline", "value": content[current]}


def lexer(content):
    if not content:
        return None
    tokens = []
    current = 0
    content_length = len(content)
    brace_group = 0
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
            current, t = lex_brace(current, content)
            if p == "(":
                brace_group += 1
                t["group"] = brace_group
            if p == ")":
                t["group"] = brace_group
                brace_group -= 1

            tokens.append(t)
        elif p == "\n":
            current, t = lex_newline(current, content)
            tokens.append(t)
        else:
            current, t = lex_word(current, content)
            tokens.append(t)
    return tokens


def parser(tokens):
    if not tokens:
    	return None
    ast = {}
    ast["sql_stmts"] = sql_stmts = []
    brace_groups = []

    parameter_rx = re.compile(r"\s*(?P<name>[\$\_\.A-Za-z0-9]+)(\s+(?P<type>\w+))?\s*")
    sql_rx = re.compile(r"--sql\(\s*(?P<name>\w+)?\s*\)--")

    sql_stmt = {"content": [], "parameters": []}

    tc = 0
    while True:
        if len(tokens) <= tc:
            break

        token = tokens[tc]
        v = token["value"]
        t = token["type"]

        if t == "parameter":
            parameter_name = v[2:len(v) - 2].lstrip().rstrip().lower()
            token["name"] = parameter_name
            sql_stmt["parameters"].append(parameter_name)

            if len(tokens) > (tc + 4):
                token2 = tokens[tc + 1]
                token3 = tokens[tc + 2]
                token4 = tokens[tc + 3]
                token5 = tokens[tc + 4]

                if token2["type"] == "space" and token3["value"] == "is" and token4["type"] == "space" and token5["value"] == "null":
                    token = {"type": "parameter", "name": parameter_name, "value": "{{" + parameter_name + "}} is null","nullable": True}
                    tc += 4

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
                tc += 1
                continue
            else:
                token["type"] = "sql"
                if len(sql_stmt):
                    if len([x for x in sql_stmt["content"] if x["type"] == "word"]):
                        sql_stmts.append(sql_stmt)
                    sql_stmt = {"content": [], "parameters": []}
                    m = sql_rx.search(v)
                    if m:
                        d = m.groupdict()
                        sql_stmt["connection"] = d["name"]
                tc += 1
                continue

        if t == "brace":
            exists = [x for x in brace_groups if x["group"] == token["group"]]
            if not exists:
                brace_groups.append(token)
                token["content"] = []
            else:
                group = exists[0]
                group["content"].append(token["value"])
                group["content"] = "".join(group["content"])
                brace_groups.remove(exists[0])

        if brace_groups:
            for g in brace_groups:
                g["content"].append(token["value"])

        sql_stmt["content"].append(token)

        tc = tc + 1

    if len([x for x in sql_stmt["content"] if x["type"] == "word"]):
        sql_stmts.append(sql_stmt)

    declaration_parameters = None
    if "declaration" in ast:
        declaration = ast["declaration"]
        if "parameters" in declaration:
            declaration["parameters"] = {x["name"]: x for x in declaration["parameters"]}
        else:
            declaration["parameters"] = {}
        declaration_parameters = declaration["parameters"]
    else:
        ast["declaration"] = {"parameters":{}}

    if declaration_parameters:
        for sql_stmt in sql_stmts:
            parameters = []
            if "parameters" in sql_stmt:
                for p in sql_stmt["parameters"]:
                    if p in declaration_parameters:
                        parameters.append(declaration_parameters[p])
                    else:
                        parameters.append({"name": p})

            sql_stmt["parameters"] = parameters

    possible_null_parameter_rx = re.compile("^\(\s*{{(?P<name>[A-Za-z0-9_.$-]*?)}}\s+is\s+null\s+or", re.IGNORECASE)
    for sql_stmt in sql_stmts:
        sql_stmt["nullable"] = []
        for token in sql_stmt["content"]:
            if token["type"] == "brace" and "content" in token:
                m = possible_null_parameter_rx.search(token["content"])
                if m:
                    d = m.groupdict()
                    sql_stmt["nullable"].append(d["name"])
                    token["nullable_parameter"] = d["name"]

                del token["content"]

        if not sql_stmt["nullable"]:
            del sql_stmt["nullable"]

    if not ast["sql_stmts"]:
        del ast["sql_stmts"]

    return ast


def compile_sql(sql_stmt, nulls, char):
    if "parameters" in sql_stmt:
        parameters_meta = {x["name"]: x for x in sql_stmt["parameters"]}
    else:
        parameters_meta = None

    stmt = sql_stmt["content"]
    tokens = []
    parameters = []
    group = None
    for token in stmt:
        if token["type"] == "brace":
            if not group and "nullable_parameter" in token:
                for p in nulls:
                    if p == token["nullable_parameter"]:
                        tokens.append("1 = 1")
                        group = token["group"]
                        continue
            else:
                if group == token["group"]:
                    group = None
                    continue

        if group:
            continue

        if token["type"] == "parameter":
            if "nullable" in token:
                tokens.append("1 = 2")
            else:
                tokens.append(char)
                parameters.append(parameters_meta[token["name"]])
        else:
            tokens.append(token["value"])

    return {"content": "".join(tokens), "parameters": parameters}


#with open("./serve/api/film/get/$.data.sql", "r") as file:
#    sqlx = file.read()


#output = parser(lexer(sqlx))
#print(json.dumps(output, indent=3))
#print(concat(output["sql_stmts"][0], [""], "?")["content"])
