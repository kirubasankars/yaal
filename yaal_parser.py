# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import re

from yaal_errors import SortDirError

KNOWN_PARAM_TYPES = frozenset({"integer", "string", "float", "bool", "blob"})

_WS_TOKEN_TYPES = frozenset({"space", "newline"})
_SORT_KEY_RX = re.compile(r"^\w+$")
_ORDER_BY_CLAUSE_END = frozenset({
    "limit", "offset", "fetch", "for", "union", "except", "intersect", ")", ";",
})

# Allowlisted client-facing dir() values -> spliced SQL suffix. NULLS placement is
# client-controlled here (not per-author-key) since these are fixed keywords, never
# identifiers -- no injection risk. Note: MySQL has no NULLS FIRST/LAST syntax; a
# *_nulls_first/*_nulls_last value will raise a real SQL error on that engine.
_DIR_VOCAB = {
    "asc": "ASC",
    "desc": "DESC",
    "asc_nulls_first": "ASC NULLS FIRST",
    "asc_nulls_last": "ASC NULLS LAST",
    "desc_nulls_first": "DESC NULLS FIRST",
    "desc_nulls_last": "DESC NULLS LAST",
}


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


def _tokens_to_sql(tokens):
    return "".join(t.get("value", "") for t in tokens)


def _body_contains_sort_or_dir_call(tokens):
    n = len(tokens)
    i = 0
    while i < n:
        t = tokens[i]
        if t["type"] == "word" and t["value"].lower() in ("sort", "dir"):
            j = _skip_ws_tokens(tokens, i + 1)
            if j < n and tokens[j]["type"] == "brace" and tokens[j]["value"] == "(":
                return True
        i += 1
    return False


def _parse_sort_body(body):
    """Parse sort({{param}}, key = expr, ...) → (param_name, choices_dict)."""
    if _body_contains_sort_or_dir_call(body):
        raise TypeError("nested sort(...)/dir(...) is not allowed")

    i = _skip_ws_tokens(body, 0)
    n = len(body)
    if i >= n or body[i]["type"] != "parameter":
        raise TypeError("sort(...) requires {{param}} as the first argument")

    param_name = _parameter_name_from_token(body[i])
    param_count = sum(1 for t in body if t["type"] == "parameter")
    if param_count != 1:
        raise TypeError("sort(...) requires exactly one {{param}}")

    i = _skip_ws_tokens(body, i + 1)
    if i >= n:
        raise TypeError("sort(...) requires at least one key = expr pair")

    # Leading comma after param (usual style: sort({{p}}, key = expr))
    if body[i]["type"] == "word" and body[i]["value"] == ",":
        i = _skip_ws_tokens(body, i + 1)
    elif body[i]["type"] == "word" and body[i]["value"].startswith(","):
        # Rare: ",key" glued — not supported; keys must be separate words.
        raise TypeError("invalid sort(...) argument list")

    if i >= n:
        raise TypeError("sort(...) requires at least one key = expr pair")

    choices = {}
    while i < n:
        if body[i]["type"] != "word" or not _SORT_KEY_RX.match(body[i]["value"]):
            raise TypeError(
                "sort(...) keys must be word characters (\\w+), got "
                + repr(body[i].get("value"))
            )
        key = body[i]["value"].lower()
        if key in choices:
            raise TypeError("duplicate sort(...) key: " + key)

        i = _skip_ws_tokens(body, i + 1)
        if i >= n or body[i]["type"] != "word" or body[i]["value"] != "=":
            raise TypeError("sort(...) expected key = expr")

        i = _skip_ws_tokens(body, i + 1)
        if i >= n:
            raise TypeError("sort(...) expected expression after =")

        expr_tokens = []
        depth = 0
        while i < n:
            t = body[i]
            if t["type"] == "brace":
                if t["value"] == "(":
                    depth += 1
                elif t["value"] == ")":
                    depth -= 1
                expr_tokens.append(t)
                i += 1
                continue

            if depth == 0 and t["type"] == "word":
                val = t["value"]
                if val == ",":
                    i += 1
                    break
                if val.endswith(",") and "," not in val[:-1]:
                    # Trailing comma glued to last expr token: u.user_name,
                    trimmed = dict(t)
                    trimmed["value"] = val[:-1]
                    if trimmed["value"] != "":
                        expr_tokens.append(trimmed)
                    i += 1
                    break

            expr_tokens.append(t)
            i += 1

        # Trim trailing whitespace from expr
        while expr_tokens and expr_tokens[-1]["type"] in _WS_TOKEN_TYPES:
            expr_tokens.pop()
        while expr_tokens and expr_tokens[0]["type"] in _WS_TOKEN_TYPES:
            expr_tokens.pop(0)
        expr_sql = _tokens_to_sql(expr_tokens).strip()
        if not expr_sql:
            raise TypeError("sort(...) empty expression for key " + key)
        choices[key] = expr_sql
        i = _skip_ws_tokens(body, i)

    if not choices:
        raise TypeError("sort(...) requires at least one key = expr pair")
    return param_name, choices


def _parse_dir_body(body):
    """Parse dir({{param}}) → param_name."""
    if _body_contains_sort_or_dir_call(body):
        raise TypeError("nested sort(...)/dir(...) is not allowed")

    i = _skip_ws_tokens(body, 0)
    n = len(body)
    if i >= n or body[i]["type"] != "parameter":
        raise TypeError("dir(...) requires exactly one {{param}}")

    param_name = _parameter_name_from_token(body[i])
    param_count = sum(1 for t in body if t["type"] == "parameter")
    if param_count != 1:
        raise TypeError("dir(...) requires exactly one {{param}}")

    i = _skip_ws_tokens(body, i + 1)
    if i < n:
        raise TypeError("dir(...) does not accept key = expr pairs")
    return param_name


def _desugar_sort_dir_tokens(tokens):
    """Replace sort(...)/dir(...) with structured tokens."""
    if not tokens:
        return tokens

    result = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok["type"] == "word" and tok["value"].lower() in ("sort", "dir"):
            kind = tok["value"].lower()
            j = _skip_ws_tokens(tokens, i + 1)
            if j < n and tokens[j]["type"] == "brace" and tokens[j]["value"] == "(":
                group = tokens[j]["group"]
                k = j + 1
                while k < n:
                    t = tokens[k]
                    if t["type"] == "brace" and t["value"] == ")" and t.get("group") == group:
                        break
                    k += 1
                else:
                    raise TypeError("unclosed " + kind + "(...)")

                body = tokens[j + 1:k]
                if kind == "sort":
                    if not body or all(t["type"] in _WS_TOKEN_TYPES for t in body):
                        raise TypeError("sort() empty / no param")
                    param_name, choices = _parse_sort_body(body)
                    result.append({
                        "type": "sort",
                        "value": "",
                        "param": param_name,
                        "choices": choices,
                    })
                else:
                    if not body or all(t["type"] in _WS_TOKEN_TYPES for t in body):
                        raise TypeError("dir() empty / no param")
                    param_name = _parse_dir_body(body)
                    result.append({
                        "type": "dir",
                        "value": "",
                        "param": param_name,
                    })
                i = k + 1
                continue

        result.append(tok)
        i += 1
    return result


def _is_order_by_clause_end_word(token):
    return token["type"] == "word" and token["value"].strip().lower() in _ORDER_BY_CLAUSE_END


def _split_order_by_terms(content, start_idx):
    """Split an ORDER BY body into comma-separated terms.

    Depth-aware: commas inside parens (e.g. `COALESCE(a, b)`) do not split a term.
    A comma glued onto the end of a word token (e.g. `u.user_id,`) is trimmed off
    the term, mirroring the sort(...) key = expr glued-comma handling.

    Returns (terms, clause_end_idx, trailing_ws) where each term is a list of
    the original tokens (leading/trailing whitespace trimmed), clause_end_idx
    is the index of the first token after the clause (its clause-end token, or
    len(content)), and trailing_ws is the whitespace text trimmed off the end
    of the last term (i.e. the gap between the clause body and clause_end_idx)
    -- callers that splice replacement text must re-append it so a following
    clause-end word/paren doesn't get glued onto the replacement.
    """
    n = len(content)
    terms = []
    current = []
    depth = 0
    i = start_idx
    while i < n:
        t = content[i]
        if depth == 0 and (
            (t["type"] == "brace" and t["value"] == ")") or _is_order_by_clause_end_word(t)
        ):
            break

        if t["type"] == "brace":
            if t["value"] == "(":
                depth += 1
            elif t["value"] == ")":
                depth -= 1
            current.append(t)
            i += 1
            continue

        if depth == 0 and t["type"] == "word":
            val = t["value"]
            if val == ",":
                terms.append(current)
                current = []
                i += 1
                continue
            if val.endswith(",") and "," not in val[:-1]:
                trimmed = dict(t)
                trimmed["value"] = val[:-1]
                if trimmed["value"] != "":
                    current.append(trimmed)
                terms.append(current)
                current = []
                i += 1
                continue

        current.append(t)
        i += 1

    terms.append(current)

    tail = current
    trailing_ws_tokens = []
    while tail and tail[-1]["type"] in _WS_TOKEN_TYPES:
        trailing_ws_tokens.insert(0, tail[-1])
        tail = tail[:-1]
    trailing_ws = "".join(t.get("value", "") for t in trailing_ws_tokens)

    def _trim(term):
        while term and term[0]["type"] in _WS_TOKEN_TYPES:
            term = term[1:]
        while term and term[-1]["type"] in _WS_TOKEN_TYPES:
            term = term[:-1]
        return term

    return [_trim(term) for term in terms], i, trailing_ws


def _validate_dynamic_order_by(content, method):
    """v2: at most one dynamic sort()/dir() term per ORDER BY; other comma-separated
    terms (a static tiebreaker before or after it) are ordinary author SQL.

    Multiple sort()/dir() pairs are allowed in one statement (e.g. a subquery's
    ORDER BY plus the outer query's ORDER BY), but each must use a distinct
    {{param}} -- resolution is keyed by param name for the whole statement, so
    reusing one param across two sort() calls (each with its own choices) would
    silently resolve to only one of them.
    """
    seen_sort_params = {}
    for tok in content:
        if tok["type"] != "sort":
            continue
        param = tok.get("param")
        if param in seen_sort_params:
            raise TypeError(
                "{{"
                + param
                + "}} is used in more than one sort(...) in "
                + method
                + ".sql -- each sort() must use a distinct param"
            )
        seen_sort_params[param] = True

    n = len(content)
    i = 0
    while i < n:
        t = content[i]
        if t["type"] == "word" and t["value"].lower() == "order":
            j = _skip_ws_tokens(content, i + 1)
            if j < n and content[j]["type"] == "word" and content[j]["value"].lower() == "by":
                k = _skip_ws_tokens(content, j + 1)
                terms, end_idx, _trailing_ws = _split_order_by_terms(content, k)

                for term in terms:
                    if not term:
                        raise TypeError(
                            "empty ORDER BY term in " + method + ".sql"
                        )

                dynamic_terms = [
                    term for term in terms if any(tok["type"] in ("sort", "dir") for tok in term)
                ]
                if len(dynamic_terms) > 1:
                    raise TypeError(
                        "only one dynamic sort()/dir() term is allowed per ORDER BY in "
                        + method
                        + ".sql"
                    )
                if len(dynamic_terms) == 1:
                    dyn = dynamic_terms[0]
                    sort_positions = [idx for idx, tok in enumerate(dyn) if tok["type"] == "sort"]
                    dir_positions = [idx for idx, tok in enumerate(dyn) if tok["type"] == "dir"]
                    if len(sort_positions) != 1:
                        raise TypeError(
                            "ORDER BY dynamic term must contain exactly one sort(...) in "
                            + method
                            + ".sql"
                        )
                    if len(dir_positions) > 1:
                        raise TypeError(
                            "ORDER BY dynamic term must contain at most one dir(...) in "
                            + method
                            + ".sql"
                        )
                    non_ws = [idx for idx, tok in enumerate(dyn) if tok["type"] not in _WS_TOKEN_TYPES]
                    if dir_positions:
                        s_pos = non_ws.index(sort_positions[0])
                        d_pos = non_ws.index(dir_positions[0])
                        if d_pos != s_pos + 1 or len(non_ws) != 2:
                            raise TypeError(
                                "ORDER BY with sort()/dir() must not include other terms in "
                                + method
                                + ".sql"
                            )
                    elif len(non_ws) != 1:
                        raise TypeError(
                            "ORDER BY with sort()/dir() must not include other terms in "
                            + method
                            + ".sql"
                        )
                i = end_idx
                continue
        i += 1


def _find_paired_dir_param(content, sort_index):
    """If a dir(...) token immediately follows content[sort_index] (ws only
    between), return its param name; else None."""
    i = _skip_ws_tokens(content, sort_index + 1)
    if i < len(content) and content[i]["type"] == "dir":
        return content[i]["param"]
    return None


def resolve_sort_dir_values(sql_stmt, input_shape):
    """
    Resolve sort()/dir() runtime values from input_shape.

    `$args.sort` may be a single key or a comma-separated list of keys
    (multi-column dynamic sort). `$args.dir` is matched to those keys by
    position (comma-separated); missing trailing entries default to "asc".
    Allowed dir values: asc, desc, asc_nulls_first, asc_nulls_last,
    desc_nulls_first, desc_nulls_last (case-insensitive).

    Returns sort_map where sort_map[param] is either:
      None                          -- elide this dynamic ORDER BY term
      "<expr1> <DIR1>, <expr2> ..." -- ready-to-splice combined SQL (direction
                                       already resolved; no separate dir_map)
    Raises SortDirError for unknown/invalid non-null values (soft error).
    """
    sort_map = {}
    content = sql_stmt.get("content") or []
    for idx, token in enumerate(content):
        if token["type"] != "sort":
            continue
        param = token["param"]
        raw_sort = input_shape.get_prop(param) if input_shape is not None else None
        if raw_sort is None:
            sort_map[param] = None
            continue
        if not isinstance(raw_sort, str):
            raw_sort = str(raw_sort)

        choices = token["choices"]
        keys = [k.strip().lower() for k in raw_sort.split(",")]
        seen = set()
        for key in keys:
            if key == "":
                raise SortDirError("unknown sort key: " + repr(raw_sort))
            if key not in choices:
                raise SortDirError("unknown sort key: " + key)
            if key in seen:
                raise SortDirError("duplicate sort key: " + key)
            seen.add(key)

        dir_param = _find_paired_dir_param(content, idx)
        raw_dir = input_shape.get_prop(dir_param) if dir_param and input_shape is not None else None
        if raw_dir is None:
            dirs = []
        else:
            if not isinstance(raw_dir, str):
                raw_dir = str(raw_dir)
            dirs = [d.strip().lower() for d in raw_dir.split(",")]
        if len(dirs) > len(keys):
            raise SortDirError(
                "too many dir values for "
                + str(len(keys))
                + " sort key(s): "
                + raw_dir
            )
        while len(dirs) < len(keys):
            dirs.append("asc")

        resolved = []
        for d in dirs:
            if d not in _DIR_VOCAB:
                raise SortDirError("unknown sort direction: " + d)
            resolved.append(_DIR_VOCAB[d])

        sort_map[param] = ", ".join(
            choices[k] + " " + d for k, d in zip(keys, resolved)
        )
    return sort_map


def _split_parameter_header_segments(inner):
    """Split header body on commas, respecting single-quoted string literals."""
    segments = []
    buf = []
    i = 0
    n = len(inner)
    in_quote = False
    while i < n:
        ch = inner[i]
        if in_quote:
            buf.append(ch)
            if ch == "\\" and i + 1 < n:
                buf.append(inner[i + 1])
                i += 2
                continue
            if ch == "'":
                in_quote = False
            i += 1
            continue
        if ch == "'":
            in_quote = True
            buf.append(ch)
            i += 1
            continue
        if ch == ",":
            segments.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if in_quote:
        raise TypeError("unclosed string literal in parameter header default")
    segments.append("".join(buf))
    return segments


def _parse_header_default_literal(raw, param_type, param_name, method):
    """Parse `= <literal>` value for a typed parameter header default."""
    text = raw.strip()
    if param_type == "blob":
        raise TypeError(
            "default values are not supported for blob parameter {{"
            + param_name
            + "}} in "
            + method
            + ".sql"
        )
    if param_type == "integer":
        if not re.fullmatch(r"-?\d+", text):
            raise TypeError(
                "invalid integer default '"
                + text
                + "' for {{"
                + param_name
                + "}} in "
                + method
                + ".sql"
            )
        return int(text)
    if param_type == "float":
        if not re.fullmatch(r"-?\d+(\.\d+)?", text):
            raise TypeError(
                "invalid float default '"
                + text
                + "' for {{"
                + param_name
                + "}} in "
                + method
                + ".sql"
            )
        return float(text)
    if param_type == "bool":
        low = text.lower()
        if low == "true":
            return True
        if low == "false":
            return False
        raise TypeError(
            "invalid bool default '"
            + text
            + "' for {{"
            + param_name
            + "}} in "
            + method
            + ".sql"
        )
    # string: '...' or bare \w+
    if text.startswith("'"):
        if len(text) < 2 or not text.endswith("'"):
            raise TypeError(
                "unclosed string literal in default for {{"
                + param_name
                + "}} in "
                + method
                + ".sql"
            )
        out = []
        j = 1
        while j < len(text) - 1:
            if text[j] == "\\" and j + 1 < len(text) - 1:
                out.append(text[j + 1])
                j += 2
                continue
            if text[j] == "'":
                raise TypeError(
                    "invalid string default for {{"
                    + param_name
                    + "}} in "
                    + method
                    + ".sql"
                )
            out.append(text[j])
            j += 1
        return "".join(out)
    if re.fullmatch(r"\w+", text):
        return text
    raise TypeError(
        "invalid string default '"
        + text
        + "' for {{"
        + param_name
        + "}} in "
        + method
        + ".sql"
    )


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
        r"\s*(?P<name>[\$\_\.A-Za-z0-9\[\]]+)(?P<required>!)?\s+(?P<type>\w+)"
        r"(?:\s*=\s*(?P<default>.+))?\s*"
    )
    params = []
    seen = set()
    for segment in _split_parameter_header_segments(inner):
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
        default_raw = d.get("default")
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
        if param_required and default_raw is not None:
            raise TypeError(
                "required parameter {{"
                + param_name
                + "}} cannot have a default in "
                + method
                + ".sql"
            )
        if param_name in seen:
            raise TypeError(
                "duplicate parameter {{" + param_name + "}} in " + method + ".sql"
            )
        seen.add(param_name)
        decl = {
            "name": param_name,
            "type": param_type,
            "required": param_required,
        }
        if default_raw is not None:
            decl["default"] = _parse_header_default_literal(
                default_raw, param_type, param_name, method
            )
        params.append(decl)
    return params


def parser(tokens, method):
    if not tokens:
        return None

    tokens = _desugar_optional_tokens(tokens)
    tokens = _desugar_sort_dir_tokens(tokens)

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
        token_value = token.get("value", "")
        token_type = token["type"]

        if token_type in _WS_TOKEN_TYPES and not significant_seen:
            tc += 1
            continue

        if token_type in ("sort", "dir"):
            sql_stmt["parameters"].append({"name": token["param"]})

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

        _validate_dynamic_order_by(sql_stmt["content"], method)

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


def _compile_order_by(stmt, order_idx, sort_map):
    """Compile the ORDER BY clause starting at stmt[order_idx] ("order").

    Renders each comma-separated term: the dynamic term (containing sort()/
    optional dir()) is replaced by its resolved combined "expr DIR, ..."
    string (dropped entirely if that resolves to None); static terms are
    reproduced verbatim. If nothing remains, the whole clause elides.

    Returns (is_order_by, fragments, next_idx):
      is_order_by: False if stmt[order_idx] is not actually "order by" (caller
                   should fall back to ordinary per-token handling).
      fragments: None to elide the entire clause (incl. keyword), or a list of
                 string fragments to splice in -- the original "order"/"by"/
                 whitespace tokens are reused verbatim (not merged into one
                 string) so downstream WHERE/PREWHERE cleanup, which detects
                 clause boundaries by exact-matching the word "order", still
                 recognizes it.
      next_idx: index in stmt right after the clause.
    """
    n = len(stmt)
    j = _skip_ws_tokens(stmt, order_idx + 1)
    if j >= n or stmt[j]["type"] != "word" or stmt[j]["value"].lower() != "by":
        return False, None, order_idx

    k = _skip_ws_tokens(stmt, j + 1)
    terms, end_idx, trailing_ws = _split_order_by_terms(stmt, k)

    rendered = []
    for term in terms:
        sort_tokens = [tok for tok in term if tok["type"] == "sort"]
        if not sort_tokens:
            rendered.append(_tokens_to_sql(term).strip())
            continue
        expr = sort_map.get(sort_tokens[0]["param"])
        if expr is not None:
            rendered.append(expr)
        # else: this dynamic term elides; drop it (and its comma) entirely.

    if not rendered:
        return True, None, end_idx

    prefix = [t.get("value", "") for t in stmt[order_idx:k]]
    # Re-append the whitespace trimmed off the end of the clause body so a
    # following clause-end word/paren (e.g. "LIMIT") isn't glued onto it.
    return True, prefix + [", ".join(rendered), trailing_ws], end_idx


def compile_sql(sql_stmt, nulls, char, sort_map=None):
    if "parameters" in sql_stmt:
        parameters_meta = {x["name"]: x for x in sql_stmt["parameters"]}
    else:
        parameters_meta = None

    sort_map = sort_map or {}
    nulls_set = {n.lower() for n in nulls}
    stmt = sql_stmt["content"]
    tokens = []
    parameters = []
    group = None
    # After skipping a non-null "{{p}} is null" marker, drop the following " or ".
    skip_or_after_nullable = False
    idx = 0
    n = len(stmt)
    while idx < n:
        token = stmt[idx]
        if token["type"] == "word" and token["value"].lower() == "order":
            is_order_by, fragments, next_idx = _compile_order_by(stmt, idx, sort_map)
            if is_order_by:
                if fragments is None:
                    # Drop preceding whitespace so we don't leave trailing spaces before LIMIT.
                    while tokens and _is_whitespace_sql_fragment(tokens[-1]):
                        tokens.pop()
                else:
                    tokens.extend(fragments)
                idx = next_idx
                continue

        if token["type"] == "brace":
            if group is not None:
                if group == token["group"]:
                    group = None
                idx += 1
                continue

            if "nullable_parameter" in token and token["nullable_parameter"] in nulls_set:
                # Elide optional ({{param}} is null or ...) and a preceding AND/OR.
                # Sole remaining WHERE is cleaned up below (no 1 = 1 injection).
                _strip_preceding_connector(tokens)
                group = token["group"]
                skip_or_after_nullable = False
                idx += 1
                continue

        if group is not None:
            idx += 1
            continue

        if skip_or_after_nullable:
            value = token.get("value", "")
            if _is_whitespace_sql_fragment(value):
                idx += 1
                continue
            if token["type"] == "word" and value.strip().lower() == "or":
                # Stay in skip mode to also drop whitespace after "or".
                idx += 1
                continue
            skip_or_after_nullable = False

        if token["type"] == "sort":
            # Normal usage is always consumed inside an ORDER BY clause above;
            # a stray sort() outside ORDER BY (author's responsibility, unvalidated)
            # splices its resolved value here, or nothing if null.
            expr = sort_map.get(token["param"])
            if expr is not None:
                tokens.append(expr)
            idx += 1
            continue

        if token["type"] == "dir":
            # Direction is always folded into the paired sort()'s resolved value;
            # a stray dir() (no preceding sort() in the same ORDER BY term) renders
            # nothing.
            idx += 1
            continue

        if token["type"] == "parameter":
            if token.get("nullable"):
                # Param is known non-null: drop "{{p}} is null or", keep the predicate.
                skip_or_after_nullable = True
                idx += 1
                continue
            tokens.append(char)
            parameters.append(parameters_meta[token["name"]])
        else:
            tokens.append(token.get("value", ""))

        idx += 1

    tokens = _cleanup_compiled_sql(tokens)
    return {"content": "".join(tokens), "parameters": parameters}
