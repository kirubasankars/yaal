# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import re

KNOWN_PARAM_TYPES = frozenset({"integer", "string", "float", "bool", "blob"})

_WS_TOKEN_TYPES = frozenset({"space", "newline"})


def lex_dash(current, content):
    """Lex `--...` as a Yaal directive, or skip a SQL line comment."""
    content_length = len(content)
    current = current + 2  # skip --

    if current >= content_length:
        return current, None

    # Malformed header: space(s) between -- and (
    j = current
    while j < content_length and content[j] in " \t\r":
        j += 1
    if j > current and j < content_length and content[j] == "(":
        raise TypeError(
            "invalid parameter header: use --(name type)-- without space after --"
        )

    if content[current] == "(":
        token = ["-", "-", "("]
        current = current + 1
        while current < content_length:
            if (
                current + 2 < content_length
                and content[current] == ")"
                and content[current + 1] == "-"
                and content[current + 2] == "-"
            ):
                token.extend([")", "-", "-"])
                current = current + 3
                return current, {"type": "dash", "value": "".join(token)}
            token.append(content[current])
            current = current + 1
        raise TypeError("unclosed parameter header --(...)--")

    if content.startswith("sql", current):
        token = ["-", "-"]
        while current < content_length:
            p = content[current]
            p1 = content[current + 1] if current + 1 < content_length else ""
            if p == "-" and p1 == "-":
                token.extend(["-", "-"])
                current = current + 2
                return current, {"type": "dash", "value": "".join(token)}
            token.append(p)
            current = current + 1
        raise TypeError("unclosed --sql-- directive")

    # SQL line comment: discard through end of line (keep newline for the lexer)
    while current < content_length and content[current] != "\n":
        current = current + 1
    return current, None


def lex_curly_braces(current, content):
    token = []
    token.extend(["{", "{"])
    current = current + 2
    content_length = len(content)
    while True:
        if content_length <= current:
            raise TypeError("unclosed {{...}} parameter")

        p = content[current]

        if content_length > current + 1:
            p1 = content[current + 1]
        else:
            p1 = ""

        if p == "}" and p1 == "}":
            token.extend(["}", "}"])
            current = current + 2
            return current, {"type": "parameter", "value": "".join(token)}

        token.extend(content[current])
        current = current + 1


def lex_string(current, content, quote):
    token = []
    token.extend([quote])
    current = current + 1
    content_length = len(content)

    while True:
        if content_length <= current:
            raise TypeError("unclosed string literal")

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
        if p not in " \t\r":
            break
        current = current + 1
    return current, {"type": "space", "value": content[start:current]}


def lex_word(current, content):
    singles = "()'\"\n \t\r"
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
            if t is not None:
                tokens.append(t)
            continue
        elif p == "{" and p1 == "{":
            current, t = lex_curly_braces(current, content)
            tokens.append(t)
            continue
        elif p in " \t\r":
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


def _parameter_name_from_token(token):
    return token["value"][2:-2].lstrip().rstrip().lower()


def _skip_ws_tokens(tokens, i):
    n = len(tokens)
    while i < n and tokens[i]["type"] in _WS_TOKEN_TYPES:
        i += 1
    return i


def _match_is_null_or_after(tokens, param_index):
    """If tokens after param_index match `is null or` (any ws/case), return index of `null`."""
    i = _skip_ws_tokens(tokens, param_index + 1)
    if i >= len(tokens) or tokens[i]["type"] != "word" or tokens[i]["value"].lower() != "is":
        return None
    i = _skip_ws_tokens(tokens, i + 1)
    if i >= len(tokens) or tokens[i]["type"] != "word" or tokens[i]["value"].lower() != "null":
        return None
    j = _skip_ws_tokens(tokens, i + 1)
    if j >= len(tokens) or tokens[j]["type"] != "word" or tokens[j]["value"].lower() != "or":
        return None
    return i


def _desugar_optional_tokens(tokens):
    """Expand optional(expr) into ({{param}} is null or expr) for existing nullable elision."""
    if not tokens:
        return tokens

    result = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok["type"] == "word" and tok["value"].lower() == "optional":
            j = _skip_ws_tokens(tokens, i + 1)
            if j < n and tokens[j]["type"] == "brace" and tokens[j]["value"] == "(":
                open_tok = tokens[j]
                group = open_tok["group"]
                k = j + 1
                while k < n:
                    t = tokens[k]
                    if t["type"] == "brace" and t["value"] == ")" and t.get("group") == group:
                        break
                    k += 1
                else:
                    raise TypeError("unclosed optional(...)")

                body = _desugar_optional_tokens(tokens[j + 1:k])
                param_names = []
                seen = set()
                for t in body:
                    if t["type"] == "parameter":
                        name = _parameter_name_from_token(t)
                        if name not in seen:
                            seen.add(name)
                            param_names.append(name)

                if len(param_names) == 0:
                    raise TypeError("optional(...) requires exactly one {{param}} in its body")
                if len(param_names) > 1:
                    raise TypeError(
                        "optional(...) requires exactly one {{param}} in its body, found: "
                        + ", ".join(param_names)
                    )

                p = param_names[0]
                result.append(open_tok)
                result.append({"type": "parameter", "value": "{{" + p + "}}"})
                result.append({"type": "space", "value": " "})
                result.append({"type": "word", "value": "is"})
                result.append({"type": "space", "value": " "})
                result.append({"type": "word", "value": "null"})
                result.append({"type": "space", "value": " "})
                result.append({"type": "word", "value": "or"})
                result.append({"type": "space", "value": " "})
                result.extend(body)
                result.append(tokens[k])
                i = k + 1
                continue

        result.append(tok)
        i += 1
    return result


def _parse_parameter_header(token_value, method):
    if not (
        token_value.startswith("--(")
        and token_value.endswith(")--")
    ):
        return None

    inner = token_value[3:-3]
    if inner.strip() == "":
        raise TypeError("empty parameter header in " + method + ".sql")

    parameter_rx = re.compile(
        r"\s*(?P<name>[\$\_\.A-Za-z0-9\[\]]+)(?P<required>!)?\s+(?P<type>\w+)\s*"
    )
    params = []
    seen = set()
    for segment in inner.split(","):
        if segment.strip() == "":
            raise TypeError("invalid parameter declaration in " + method + ".sql")
        m = parameter_rx.fullmatch(segment)
        if not m:
            raise TypeError(
                "invalid parameter declaration '"
                + segment.strip()
                + "' in "
                + method
                + ".sql"
            )
        d = m.groupdict()
        param_name = d["name"].lstrip().rstrip().lower()
        param_type = d["type"].lower()
        param_required = bool(d.get("required"))
        if param_type not in KNOWN_PARAM_TYPES:
            raise TypeError(
                "unknown parameter type '"
                + param_type
                + "' for {{"
                + param_name
                + "}} in "
                + method
                + ".sql (expected "
                + ", ".join(sorted(KNOWN_PARAM_TYPES))
                + ")"
            )
        if param_name in seen:
            raise TypeError(
                "duplicate parameter {{" + param_name + "}} in " + method + ".sql"
            )
        seen.add(param_name)
        params.append({
            "name": param_name,
            "type": param_type,
            "required": param_required,
        })
    return params


def parser(tokens, method):
    if not tokens:
        return None

    tokens = _desugar_optional_tokens(tokens)

    ast = {}
    ast["sql_stmts"] = sql_stmts = []
    brace_groups = []

    sql_rx = re.compile(r"--sql\(\s*(?P<name>\w+)?\s*\)--")

    sql_stmt = {
        "content": [],
        "parameters": []
    }

    tc = 0
    significant_seen = False
    while True:
        if len(tokens) <= tc:
            break

        token = tokens[tc]
        token_value = token["value"]
        token_type = token["type"]

        if token_type in _WS_TOKEN_TYPES and not significant_seen:
            tc += 1
            continue

        if token_type == "parameter":
            parameter_name = token_value[2:len(token_value) - 2].lstrip().rstrip().lower()
            token["name"] = parameter_name
            sql_stmt["parameters"].append({"name": parameter_name})

            null_idx = _match_is_null_or_after(tokens, tc)
            if null_idx is not None:
                if not brace_groups:
                    raise TypeError(
                        "{{"
                        + parameter_name
                        + "}} is null or must be wrapped in parentheses in "
                        + method
                        + ".sql"
                    )
                token = {
                    "type": "parameter",
                    "name": parameter_name,
                    "value": "{{" + parameter_name + "}} is null",
                    "nullable": True
                }
                tc = null_idx

        if token_type == "dash":
            header_params = None
            if not significant_seen:
                header_params = _parse_parameter_header(token_value, method)
            if header_params is not None:
                ast["parameters"] = {x["name"]: x for x in header_params}
                significant_seen = True
                tc += 1
                continue
            else:
                token["type"] = "sql"
                if len(sql_stmt):
                    if len([x for x in sql_stmt["content"] if x["type"] == "word"]):
                        sql_stmts.append(sql_stmt)
                    # New SQL statement
                    sql_stmt = {
                        "content": [],
                        "parameters": []
                    }
                    m = sql_rx.search(token_value)
                    if m:
                        d = m.groupdict()
                        sql_stmt["connection"] = d["name"] or "db"
                significant_seen = True
                tc += 1
                continue

        if token_type == "brace":
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
        significant_seen = True

        tc = tc + 1

    if len([x for x in sql_stmt["content"] if x["type"] == "word"]):
        sql_stmts.append(sql_stmt)

    ast_parameters = None
    if "parameters" in ast:
        ast_parameters = ast["parameters"]

    for sql_stmt in sql_stmts:

        parameters = []
        if "parameters" in sql_stmt:
            for p in sql_stmt["parameters"]:
                if ast_parameters and p["name"] in ast_parameters:
                    parameters.append(ast_parameters[p["name"]])
                else:
                    raise TypeError("type missing for {{" + p["name"] + "}} in the " + method + ".sql")

        sql_stmt["parameters"] = parameters

    possible_null_parameter_rx = re.compile(
        r"^\(\s*{{(?P<name>[A-Za-z0-9_.$-]*?)}}\s+is\s+null\s+or",
        re.IGNORECASE,
    )

    for sql_stmt in sql_stmts:

        sql_stmt["nullable"] = []
        for token in sql_stmt["content"]:
            if token["type"] == "brace" and "content" in token:
                m = possible_null_parameter_rx.search(token["content"])
                if m:
                    name = m.groupdict()["name"].lower()
                    sql_stmt["nullable"].append(name)
                    token["nullable_parameter"] = name

                del token["content"]

        if not sql_stmt["nullable"]:
            del sql_stmt["nullable"]

    if not ast["sql_stmts"]:
        del ast["sql_stmts"]

    return ast


def _is_whitespace_sql_fragment(value):
    return value == "" or (isinstance(value, str) and value.isspace())


_CLAUSE_BOUNDARY = frozenset({
    "order", "group", "having", "limit", "offset", "fetch", "for",
    "union", "except", "intersect", ")", "where", "prewhere",
})

_FILTER_CLAUSES = frozenset({"where", "prewhere"})

_ONE_EQUALS_ONE_COMPACT = re.compile(r"^1\s*=\s*1$")


def _strip_preceding_connector(tokens):
    """Drop a preceding AND/OR and adjacent whitespace. Returns True if a connector was removed."""
    i = len(tokens) - 1
    while i >= 0 and _is_whitespace_sql_fragment(tokens[i]):
        i -= 1
    if i < 0 or tokens[i].strip().lower() not in ("and", "or"):
        return False
    del tokens[i:]
    while tokens and _is_whitespace_sql_fragment(tokens[-1]):
        tokens.pop()
    return True


def _next_significant(tokens, i):
    while i < len(tokens):
        if not _is_whitespace_sql_fragment(tokens[i]):
            return i, tokens[i].strip().lower()
        i += 1
    return None, None


def _match_one_equals_one(tokens, i):
    """If tokens[i:] starts with 1 = 1 (flexible whitespace), return index after match."""
    parts = []
    j = i
    while j < len(tokens) and len(parts) < 3:
        if _is_whitespace_sql_fragment(tokens[j]):
            j += 1
            continue
        parts.append((j, tokens[j].strip()))
        j += 1
        if len(parts) == 1 and _ONE_EQUALS_ONE_COMPACT.match(parts[0][1]):
            return parts[0][0] + 1
    if (
        len(parts) >= 3
        and parts[0][1] == "1"
        and parts[1][1] == "="
        and parts[2][1] == "1"
    ):
        return parts[2][0] + 1
    return None


def _trim_ws_before(tokens, i):
    while i > 0 and _is_whitespace_sql_fragment(tokens[i - 1]):
        del tokens[i - 1]
        i -= 1
    return i


def _cleanup_compiled_sql(tokens):
    """Drop empty/tautology WHERE|PREWHERE 1 = 1 left after optional-filter elision."""
    tokens = list(tokens)
    changed = True
    while changed:
        changed = False
        for i, t in enumerate(tokens):
            if _is_whitespace_sql_fragment(t):
                continue
            if t.strip().lower() not in _FILTER_CLAUSES:
                continue

            j, word = _next_significant(tokens, i + 1)
            if j is None or word in _CLAUSE_BOUNDARY:
                # Empty WHERE/PREWHERE at EOF, before ), ORDER/GROUP/WHERE/..., etc.
                old_i = i
                i = _trim_ws_before(tokens, i)
                if j is None:
                    del tokens[i:]
                else:
                    j -= old_i - i
                    del tokens[i:j]
                    # Keep a space before the next keyword (not before ')').
                    if (
                        i > 0
                        and i < len(tokens)
                        and not _is_whitespace_sql_fragment(tokens[i - 1])
                        and not _is_whitespace_sql_fragment(tokens[i])
                        and tokens[i].strip() != ")"
                    ):
                        tokens.insert(i, " ")
                changed = True
                break

            one_end = _match_one_equals_one(tokens, j)
            if one_end is None:
                # Real predicate; keep scanning for other WHERE/PREWHERE clauses.
                continue

            k, next_word = _next_significant(tokens, one_end)
            if k is None or next_word in _CLAUSE_BOUNDARY:
                # Sole WHERE/PREWHERE 1 = 1 (or before ORDER/GROUP/WHERE/...).
                old_i = i
                i = _trim_ws_before(tokens, i)
                one_end -= old_i - i
                del tokens[i:one_end]
                while i < len(tokens) and _is_whitespace_sql_fragment(tokens[i]):
                    if k is not None:
                        break
                    del tokens[i]
                if k is None:
                    while tokens and _is_whitespace_sql_fragment(tokens[-1]):
                        tokens.pop()
                changed = True
                break

            if next_word in ("and", "or"):
                # WHERE/PREWHERE 1 = 1 AND|OR rest → WHERE/PREWHERE rest
                del_end = k + 1
                while del_end < len(tokens) and _is_whitespace_sql_fragment(tokens[del_end]):
                    del_end += 1
                del tokens[j:del_end]
                changed = True
                break

            continue
    return tokens


def compile_sql(sql_stmt, nulls, char):
    if "parameters" in sql_stmt:
        parameters_meta = {x["name"]: x for x in sql_stmt["parameters"]}
    else:
        parameters_meta = None

    nulls_set = {n.lower() for n in nulls}
    stmt = sql_stmt["content"]
    tokens = []
    parameters = []
    group = None
    # After skipping a non-null "{{p}} is null" marker, drop the following " or ".
    skip_or_after_nullable = False
    for token in stmt:
        if token["type"] == "brace":
            if group is not None:
                if group == token["group"]:
                    group = None
                continue

            if "nullable_parameter" in token and token["nullable_parameter"] in nulls_set:
                # Elide optional ({{param}} is null or ...) and a preceding AND/OR.
                # Sole remaining WHERE is cleaned up below (no 1 = 1 injection).
                _strip_preceding_connector(tokens)
                group = token["group"]
                skip_or_after_nullable = False
                continue

        if group is not None:
            continue

        if skip_or_after_nullable:
            value = token.get("value", "")
            if _is_whitespace_sql_fragment(value):
                continue
            if token["type"] == "word" and value.strip().lower() == "or":
                # Stay in skip mode to also drop whitespace after "or".
                continue
            skip_or_after_nullable = False

        if token["type"] == "parameter":
            if token.get("nullable"):
                # Param is known non-null: drop "{{p}} is null or", keep the predicate.
                skip_or_after_nullable = True
                continue
            tokens.append(char)
            parameters.append(parameters_meta[token["name"]])
        else:
            tokens.append(token["value"])

    tokens = _cleanup_compiled_sql(tokens)
    return {"content": "".join(tokens), "parameters": parameters}
