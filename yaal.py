import copy
import datetime
import json
import logging
import os
import re
import sqlite3
import urllib
from collections import defaultdict

import yaml
from jsonschema import FormatChecker, Draft4Validator

from parser import parser, lexer, compile_sql
from yaal_postgres import PostgresContextManager
from yaal_mysql import MySQLContextManager

logger = logging.getLogger("yaal")
logger.setLevel(logging.INFO)

path_join = os.path.join


def _to_lower_keys(obj):
    if obj:
        if type(obj) == dict:
            return {k.lower(): v for k, v in obj.items()}
        elif type(obj) == list and type(obj[0]) == dict:
            return [_to_lower_keys(item) for item in obj]
    return obj


def _to_lower_keys_deep(obj):
    if obj:
        if type(obj) == dict:
            o = {}
            for k, v in obj.items():
                if type(v) == list or type(v) == dict:
                    o[k.lower()] = _to_lower_keys(v)
                else:
                    o[k.lower()] = v
            return o
        elif type(obj) == list and type(obj[0]) == dict:
            return [_to_lower_keys(item) for item in obj]
    return obj


def _build_twigs(branch, sql_stmts, bag):

    for twig in sql_stmts:
        for p in twig["parameters"]:
            if "type" not in p:
                raise TypeError("type missing for {{" + p + "}} in the " + branch["method"] + ".sql")

        if "connection" in twig:
            bag["connections"].append(twig["connection"])

    branch["twigs"] = sql_stmts


def _order_list_by_dots(names):
    if not names:
        return []

    dots = [x.count(".") for x in names]
    ordered = []
    for x in range(0, len(dots)):
        if len(dots) == 0:
            break

        el = min(dots)
        while True:
            try:
                idx = dots.index(el)
                ordered.append(names[idx].lower())
                del names[idx]
                del dots[idx]
            except Exception:
                break
    return ordered


def _build_branch_map_by_files(branch_map, item):
    if item == "":
        return
    dot = item.find(".")
    if dot > -1:
        path = item[0:dot]
        remaining_path = item[dot + 1:]
        if path not in branch_map:
            branch_map[path] = {}
        _build_branch_map_by_files(branch_map[path], remaining_path)
    else:
        if item not in branch_map:
            branch_map[item] = {}


def _build_trunk_map_by_files(name_list):
    trunk_map = {}
    if name_list:
        for item in name_list:
            _build_branch_map_by_files(trunk_map, item)
    return trunk_map


def _build_branch(branch, map_by_files, content_reader, payload_model, output_model, model, bag):
    _properties_str, _type_str, _partition_by_str = "properties", "type", "partition_by"
    _output_type_str, _use_parent_rows_str = "output_type", "use_parent_rows"
    _parameters_str, _twig_str, _parent_rows_str = "parameters", "twig", "parent_rows"

    path, method = branch["path"], branch["method"]
    content = content_reader.get_sql(method, path)
    branch_map = {}
    
    if _properties_str not in payload_model:
        payload_model[_properties_str] = {}

    branch["input_type"] = payload_model[_type_str]
    input_properties = payload_model[_properties_str]

    output_properties = None
    if output_model:
        if _type_str in output_model:
            branch[_output_type_str] = output_model[_type_str]
        else:
            branch[_output_type_str] = "array"

        if _properties_str in output_model:
            output_properties = output_model[_properties_str]

        if _parent_rows_str in output_model:
            branch[_use_parent_rows_str] = output_model[_parent_rows_str]

        if _partition_by_str in output_model:
            branch[_partition_by_str] = output_model[_partition_by_str]

        if output_properties:
            for k in output_properties:
                v = output_properties[k]
                if type(v) == dict and _type_str in v:
                    _type = v[_type_str]
                    if _type == "object" or _type == "array":
                        branch_map[k] = {}
    else:
        branch[_output_type_str] = "array"
        branch[_use_parent_rows_str] = False

    if content:

        ast = parser(lexer(content))
        if "sql_stmts" not in ast:
            return

        branch["parameters"] = ast["declaration"]["parameters"]
            
        for k, v in branch["parameters"].items():
            if k[0] == "$" and k.find("$parent") == -1:
                set_model_def(model, k, v)
            else:
                set_model_def(payload_model, k, v)

        _build_twigs(branch, ast["sql_stmts"], bag)

    lower_branch_map = _to_lower_keys(branch_map)
    for k in map_by_files:
        if k not in lower_branch_map:
            branch_map[k] = map_by_files[k]

    branches = []
    for sub_branch_name in branch_map:
        sub_branch_map = branch_map[sub_branch_name]
        sub_branch_method = ".".join([method, sub_branch_name]).lower()
        sub_branch = {
            "name": sub_branch_name,
            "method": sub_branch_method,
            "path": path
        }

        sub_branch_output_model = None

        if sub_branch_name not in input_properties:
            input_properties[sub_branch_name] = {
                "type": "object",
                "properties": {}
            }
        sub_branch_payload_model = input_properties[sub_branch_name]

        if output_properties and sub_branch_name in output_properties:
            sub_branch_output_model = output_properties[sub_branch_name]

        sub_branch_payload_model["$parent"] = payload_model

        _build_branch(sub_branch, sub_branch_map, content_reader, sub_branch_payload_model, sub_branch_output_model,
                      model, bag)

        del sub_branch_payload_model["$parent"]

        branches.append(sub_branch)

    if branches:
        branch["branches"] = branches


def set_model_def(model, prop, value):
    dot = prop.find(".")
    if dot > -1:
        path = prop[:dot]
        if path == "$parent":
            if "payload" in model:
                model = model["payload"]["$parent"]
            else:
                if "$parent" in model:
                    model = model["$parent"]
                else:
                    model = None
        elif path == "$query":
            model = model["query"]
        elif path == "$cookie":
            model = model["cookie"]
        elif path == "$header":
            model = model["header"]
        elif path == "$path":
            model = model["path"]
        elif path == "$params":
            return
        else:
            if "payload" in model:
                model = model["payload"]
            else:
                model = model["properties"][path]
        set_model_def(model, prop[dot + 1:], value)
    else:
        _type = value.get("type")
        if not _type:
            _type = "string"
        if model and "properties" in model:
            if model and prop not in model["properties"]:
                model["properties"][prop] = {
                    "type": _type
                }


def create_trunk(path, content_reader):
    ordered_files = _order_list_by_dots(content_reader.list_sql(path))
    if len(ordered_files) == 0:
        return None

    trunk_map = _build_trunk_map_by_files(ordered_files)
    config = content_reader.get_config(path)

    input_model_str, output_model_str = "input.model", "output.model"
    query_str, path_str, header_str, cookie_str, payload_str = "query", "path", "header", "cookie", "payload"
    payload_model, output_model = None, None
    query_model, path_model, header_model, cookie_model = None, None, None, None

    if config:
        input_model = config.get(input_model_str)
        if input_model:
            payload_model = _to_lower_keys_deep(input_model.get(payload_str))
            query_model = _to_lower_keys_deep(input_model.get(query_str))
            path_model = _to_lower_keys_deep(input_model.get(path_str))
            header_model = _to_lower_keys_deep(input_model.get(header_str))
            cookie_model = _to_lower_keys_deep(input_model.get(cookie_str))

        output_model = config.get(output_model_str)

    if not query_model:
        query_model = {
            "type": "object",
            "properties": {}
        }
    if not path_model:
        path_model = {
            "type": "object",
            "properties": {}
        }
    if not header_model:
        header_model = {
            "type": "object",
            "properties": {}
        }
    if not cookie_model:
        cookie_model = {
            "type": "object",
            "properties": {}
        }
    if not payload_model:
        payload_model = {
            "type": "object",
            "properties": {}
        }
    if not output_model:
        output_model = {
            "type": "array",
            "properties": {}
        }

    trunk = {
        "name": "$",
        "method": "$",
        "path": path,
        "model": {
            "query": query_model,
            "path": path_model,
            "header": header_model,
            "cookie": cookie_model,
            "payload": payload_model,
            "output": output_model
        }
    }

    if payload_model:
        payload_validator = Draft4Validator(schema=payload_model, format_checker=FormatChecker())
    else:
        payload_validator = None

    if query_model:
        parameter_query_validator = Draft4Validator(schema=query_model, format_checker=FormatChecker())
    else:
        parameter_query_validator = None

    if path_model:
        parameter_path_validator = Draft4Validator(schema=path_model, format_checker=FormatChecker())
    else:
        parameter_path_validator = None

    if header_model:
        parameter_header_validator = Draft4Validator(schema=header_model, format_checker=FormatChecker())
    else:
        parameter_header_validator = None

    if cookie_model:
        parameter_cookie_validator = Draft4Validator(schema=cookie_model, format_checker=FormatChecker())
    else:
        parameter_cookie_validator = None

    bag = {
        "connections": ["db"]
    }
    _build_branch(trunk, trunk_map["$"], content_reader, payload_model, output_model, trunk["model"], bag)
    trunk["connections"] = list(set(bag["connections"]))

    validators = {
        "query": parameter_query_validator,
        "path": parameter_path_validator,
        "header": parameter_header_validator,
        "cookie": parameter_cookie_validator,
        "payload": payload_validator
    }
    trunk["_validators"] = validators

    return trunk


def _execute_twigs(branch, data_providers, context, data_provider_helper):
    errors = []

    twigs = branch.get("twigs")
    action_str = "$action"
    params_str, header_str, cookie_str, error_str, break_str = "params", "header", "cookie", "error", "break"
    json_str = "json"

    rs = []
    if twigs:
        for twig in twigs:
            if "connection" in twig:
                connection = twig["connection"]
            else:
                connection = "db"
            output, output_last_inserted_id = data_providers[connection].execute(twig, context, data_provider_helper)
            context.get_prop("$params").set_prop("$last_inserted_id", output_last_inserted_id)

            if len(output) >= 1:
                output0 = output[0]
                if action_str in output0:
                    action_value = output0[action_str]
                    if action_value == error_str:
                        if "$http_status_code" in output0:
                            context.set_prop("$response.status_code", output0["$http_status_code"])
                        errors.extend(output)
                        return None, errors
                    elif action_value == json_str:
                        json_list = []
                        if type(output0[json_str]) == str:
                            for o in output:
                                json_list.append(json.loads(o[json_str]))
                        else:
                            json_list.extend([o[json_str] for o in output])
                        return json_list, None

                    elif action_value == break_str:
                        for o in output:
                            del o[action_str]
                        return output, None
                    elif action_value == params_str:
                        params = context.get_prop("$params")
                        for k, v in output0.items():
                            params.set_prop(k, v)
                    elif action_value == cookie_str:
                        cookie = context.get_prop("$response.$cookie")
                        for c in output:
                            if "name" in c and "value" in c:
                                cookie.set_prop(c["name"], c)
                    elif action_value == header_str:
                        header = context.get_prop("$response.$header")
                        for h in output:
                            if "name" in h and "value" in h:
                                header.set_prop(h["name"], h)
                else:
                    rs = output
    return rs, None


def _execute_branch(branch, is_trunk, data_providers, context, parent_rows, parent_partition_by):
    input_type, output_partition_by = branch["input_type"], branch.get("partition_by")
    use_parent_rows = branch.get("use_parent_rows")
    output = []
    data_provider_helper = DataProviderHelper()
    db_data_provider = data_providers["db"]

    try:
        if is_trunk:
            for name, data_provider in data_providers.items():
                data_provider.begin()

        if input_type == "array":
            length = int(context.get_prop("$length"))
            for i in range(0, length):
                item_ctx = context.get_prop("@" + str(i))
                rs, errors = _execute_twigs(branch, data_providers, item_ctx, data_provider_helper)
                output.extend(rs)
                if errors:
                    return None, errors

        elif input_type == "object":
            output, errors = _execute_twigs(branch, data_providers, context, data_provider_helper)
            if errors:
                return None, errors

        if use_parent_rows and not parent_partition_by:
            raise Exception("parent _partition_by is can't be empty when child wanted to use parent rows")

        if use_parent_rows:
            output = copy.deepcopy(parent_rows)

        branches = branch.get("branches")
        if branches:
            for branch_descriptor in branches:
                branch_name = branch_descriptor["name"]
                sub_node_shape = None
                if context:
                    sub_node_shape = context.get_prop(branch_name.lower())

                sub_node_output, errors = _execute_branch(branch_descriptor, False, data_providers, sub_node_shape,
                                                          output,
                                                          output_partition_by)
                if errors:
                    return None, errors

                if not branch.get("twigs") and not output:
                    output.append({})

                if not output_partition_by:
                    for row in output:
                        row[branch_name] = sub_node_output
                else:
                    sub_node_groups = defaultdict(list)
                    for row in sub_node_output:
                        sub_node_groups[row[output_partition_by]].append(row)

                    groups = defaultdict(list)
                    for row in output:
                        groups[row[output_partition_by]].append(row)

                    _output = []
                    for idx, rows in groups.items():
                        row = rows[0]
                        partition_by = row[output_partition_by]
                        row[branch_name] = sub_node_groups[partition_by]
                        _output.append(row)
                    output = _output
        else:
            pass

        if is_trunk:
            db_data_provider.end()
            for name, data_provider in data_providers.items():
                if name != "db":
                    data_provider.end()

    except Exception as e:
        # logger.error(e)
        if is_trunk:
            try:
                db_data_provider.error()
            except Exception:
                pass

            for name, data_provider in data_providers.items():
                if name != "db":
                    try:
                        data_provider.error()
                    except Exception:
                        pass
        raise e

    return output, None


def _output_mapper(output_type, output_modal, branches, result):
    mapped_result = []

    _type_str, _mapped_str = "type", "mapped"

    output_model = output_modal
    if output_model and "properties" in output_model:
        output_properties = output_model["properties"]
    else:
        output_properties = None

    output_type = output_type

    for row in result:
        mapped_obj = {}
        mapped_tree = {}
        if branches:
            for branch_descriptor in branches:

                branch_name = branch_descriptor["name"]
                branch_output_type = branch_descriptor["output_type"]

                if output_properties and branch_name in output_properties:
                    branch_output_model = output_properties[branch_name]
                else:
                    branch_output_model = None

                branch_descriptor_branches = branch_descriptor.get("branches")

                if branch_name in row:
                    mapped_tree[branch_name] = _output_mapper(branch_output_type, branch_output_model,
                                                              branch_descriptor_branches, row[branch_name])

        if output_properties:
            prop_count = 0
            for k, v in output_properties.items():

                _mapped, _type = None, None

                if type(v) == str:
                    _mapped = v
                if type(v) == dict:
                    _mapped = v.get(_mapped_str)
                    _type = v.get(_type_str)

                if _mapped:
                    if _mapped in row:
                        mapped_obj[k] = row[_mapped]
                        prop_count = prop_count + 1
                    else:
                        raise Exception(_mapped + " _mapped column missing from row")

                if _type and (_type == "array" or _type == "object"):
                    mapped_obj[k] = mapped_tree[k]

            if prop_count == 0:
                mapped_obj = row
        else:
            mapped_obj = row

        for k, v in mapped_tree.items():
            mapped_obj[k] = v

        mapped_result.append(mapped_obj)

    if output_type == "object":
        if len(mapped_result) > 0:
            mapped_result = mapped_result[0]
        else:
            mapped_result = {}
    return mapped_result


def get_result(descriptor, get_data_provider, ctx):
    errors = ctx.get_prop("$request").validate(True)
    errors.extend(ctx.validate(False))

    status_code_str = "$response.status_code"
    if errors:
        ctx.set_prop(status_code_str, 400)
        return {"errors": errors}

    data_providers = {}
    for con in descriptor["connections"]:
        data_providers[con] = get_data_provider(con)

    rs, errors = _execute_branch(descriptor, True, data_providers, ctx, [], None)

    if errors:
        status_code = ctx.get_prop(status_code_str)
        if not status_code:
            ctx.set_prop(status_code_str, 400)
        return {"errors": errors}

    rs = _output_mapper(descriptor["output_type"], descriptor["model"]["output"], descriptor.get("branches"), rs)
    ctx.set_prop(status_code_str, 200)

    return rs


def _default_date_time_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def get_result_json(descriptor, get_data_providers, context):
    return json.dumps(get_result(descriptor, get_data_providers, context), default=_default_date_time_converter)


def get_descriptor_json(descriptor):
    d = copy.deepcopy(descriptor)
    del d["_validators"]
    return json.dumps(d)


def create_context(descriptor, payload=None, query=None, path_values=None, header=None, cookie=None):
    model_str = "model"
    query_str, path_str, header_str, cookie_str, payload_str = "query", "path", "header", "cookie", "payload"

    model = descriptor.get(model_str)
    validators = descriptor.get("_validators")
    if model and validators:
        if query_str in model:
            query_schema = model[query_str]
            query_validator = validators[query_str]
        else:
            query_schema = None
            query_validator = None

        if path_str in model:
            path_schema = model[path_str]
            path_validator = validators[path_str]
        else:
            path_schema = None
            path_validator = None

        if header_str in model:
            header_schema = model[header_str]
            header_validator = validators[header_str]
        else:
            header_schema = None
            header_validator = None

        if cookie_str in model:
            cookie_schema = model[cookie_str]
            cookie_validator = validators[cookie_str]
        else:
            cookie_schema = None
            cookie_validator = None

        if payload_str in model:
            payload_schema = model[payload_str]
            payload_validator = validators[payload_str]
        else:
            payload_schema = None
            payload_validator = None
    else:
        query_schema = None
        query_validator = None
        path_schema = None
        path_validator = None
        header_schema = None
        header_validator = None
        cookie_schema = None
        cookie_validator = None
        payload_schema = None
        payload_validator = None

    query_shape = Shape(query_schema, None, None, None, query_validator)
    if query:
        for k, v in query.items():
            query_shape.set_prop(k.lower(), v)

    path_shape = Shape(path_schema, None, None, None, path_validator)
    if path_values:
        for k, v in path_values.items():
            path_shape.set_prop(k.lower(), v)

    header_shape = Shape(header_schema, header, None, None, header_validator)
    cookie_shape = Shape(cookie_schema, cookie, None, None, cookie_validator)

    request_extras = {
        "$query": query_shape,
        "$path": path_shape,
        "$header": header_shape,
        "$cookie": cookie_shape
    }
    request_shape = Shape({}, None, None, request_extras, None)

    response_extras = {
        "$header": Shape(None, None, None, None, None),
        "$cookie": Shape(None, None, None, None, None)
    }
    response_shape = Shape({}, None, None, response_extras, None)

    params = {
        "path": descriptor["path"]
    }
    params_shape = Shape({}, params, None, None, None)

    extras = {
        "$params": params_shape,
        "$query": query_shape,
        "$path": path_shape,
        "$header": header_shape,
        "$cookie": cookie_shape,
        "$request": request_shape,
        "$response": response_shape
    }

    return Shape(payload_schema, payload, None, extras, payload_validator)


def _build_routes(routes):
    _routes = []
    if routes:
        for r in routes:
            if "route" in r and "descriptor" in r:
                p = "^" + re.sub(r"{(.*?)}", r"(?P<\1>[^/]+)", r["route"]) + "/?$"
                _routes.append({"route": re.compile(p), "descriptor": r["descriptor"], "path": r["route"]})
        return _routes


def _parse_rfc1738_args(connection_url):
    pattern = re.compile(r'''(?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>[^/]*))?
            @)?
            (?:
                (?P<host>[^/:]*)
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            ''', re.X)

    m = pattern.match(connection_url)
    if m is not None:
        components = m.groupdict()
        if components['database'] is not None:
            tokens = components['database'].split('?', 2)
            components['database'] = tokens[0]
            query = (len(tokens) > 1 and dict(urllib.parse_qsl(tokens[1]))) or None
            # Py2K
            if query is not None:
                query = dict((k.encode('ascii'), query[k]) for k in query)
            # end Py2K
        else:
            query = None
        components['query'] = query

        if components['password'] is not None:
            components['password'] = urllib.parse.unquote_plus(components['password'])

        return components.pop('name'), components
    else:
        raise Exception("Could not parse rfc1738 URL from string '%s'" % connection_url)


def build_api_meta(y):
    root_path = y.get_root_path()
    all_paths = sorted([x[0] for x in os.walk(root_path)])
    paths = {}
    method_rx = re.compile("/(?P<method>get|put|post|delete)$")

    for p in all_paths:
        m = method_rx.search(p)
        if m:
            mdict = m.groupdict()
            p = p.replace(root_path + "/", "")
            p = method_rx.sub("", p)
            path = "/api/" + p

            descriptor = y.get_descriptor(p + "/" + mdict["method"], p + "/" + mdict["method"])
            if descriptor:

                if path not in paths:
                    paths[path] = {}

                if len(descriptor["model"]["payload"]["properties"]) > 0:
                    payload = descriptor["model"]["payload"]
                else:
                    payload = None

                paths[path][mdict["method"]] = {
                    "parameters": [],
                    "requestBody": {
                        "content": {
                            "application/json": payload
                        }
                    },
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": descriptor["model"]["output"]
                            }
                        }
                    }
                }

                parameters = paths[path][mdict["method"]]["parameters"]

                for k, v in descriptor["model"]["query"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "query",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["header"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "header",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["cookie"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "cookie",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["path"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "path",
                        "schema": {
                            "type": v["type"]
                        }
                    })

        routes = y.get_routes()
        if routes:
            for r in routes:
                p = "/api/" + r["descriptor"]
                path = "/api/" + r["path"]
                if p in paths:
                    paths[path] = paths[p]
                    del paths[p]

    return paths


class Shape:

    def __init__(self, schema, data, parent_shape, extras, validator):
        self._array = False
        self._object = False

        self._data = data or {}
        self._o_data = data or {}
        self._parent = parent_shape
        self._schema = schema
        self._input_properties = None
        input_properties = None
        self._index = 0
        self._validator = validator

        self._extras = extras

        if data is not None and ("$parent" in data or "$length" in data):
            raise Exception("$parent or $length is reversed keywords. You can't use them.")

        schema = schema or {}

        _properties_str = "properties"
        if _properties_str in schema:
            input_properties = schema[_properties_str]
            self._input_properties = input_properties

        _type_str = "type"
        if _type_str in schema:
            _type = schema[_type_str]
            if _type == "array":
                self._array = True
                if data and type(data) != list:
                    raise TypeError("input expected as array. object is given.")
            else:
                self._object = True
                if data:
                    if type(data) != dict:
                        raise TypeError("input expected as object. array is given.")
                    else:
                        self._data = _to_lower_keys(data)

        if self._array:
            shapes = []
            schema[_type_str] = "object"
            idx = 0
            for item in self._data:
                s = Shape(schema, item, self, extras, None)
                s._index = idx
                shapes.append(s)
                idx = idx + 1
            schema[_type_str] = "array"
        else:
            shapes = {}
            if input_properties:
                for k, v in input_properties.items():
                    if type(v) == dict:
                        _type_value = v.get(_type_str)
                        if _type_value and _type_value == "array" or _type_value == "object":
                            shapes[k.lower()] = Shape(v, self._data.get(k.lower()), self, extras, None)

        self._shapes = shapes

    def get_prop(self, prop):
        extras = self._extras
        shapes = self._shapes
        data = self._data
        parent = self._parent

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot + 1:]

            if path[0] == "$":
                if path == "$parent":
                    return parent.get_prop(remaining_path)

                if extras:
                    if path in extras:
                        return extras[path].get_prop(remaining_path)

            if self._array:
                idx = int(path[1:])
                return shapes[idx].get_prop(remaining_path)

            return shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":

                if prop == "$json" or prop == "$parent" or prop == "$length" or prop == "$index":
                    if prop == "$json":
                        return json.dumps(self.get_data())
                    if prop == "$parent":
                        return parent
                    if prop == "$length":
                        return len(data)
                    if prop == "$index":
                        return self._index

                if extras:
                    if prop in extras:
                        return extras[prop]

            if self._array:
                idx = int(prop[1:])
                return shapes[idx]

            if prop in shapes:
                return shapes[prop]

            if prop in data:
                return data[prop]

            if self._input_properties is not None and prop in self._input_properties:
                default_str = "default"
                input_type = self._input_properties[prop]
                if default_str in input_type:
                    return input_type[default_str]

            return None

    def set_prop(self, prop, value):
        shapes = self._shapes

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot + 1:]

            if path in shapes:
                if self._array:
                    idx = int(path[1:])
                    return shapes[idx].set_prop(remaining_path, value)
                else:
                    return shapes[path].set_prop(remaining_path, value)
            else:
                if path in self._extras:
                    self._extras[path].set_prop(remaining_path, value)
        else:
            v = self.check_and_cast(prop, value)
            self._data[prop.lower()] = v
            self._o_data[prop] = v

    def validate(self, include_extras):
        errors = []

        extras = self._extras
        if extras and include_extras:
            for name, extra in extras.items():
                for x in extra.validate(include_extras):
                    errors.append(x)

        if self._validator:
            error_list = list(self._validator.iter_errors(self._data))
            if error_list:
                for x in error_list:
                    p = None
                    if len(x.path) > 0:
                        p = x.path[0]

                    m = {
                        "path": p,
                        "message": x.message
                    }
                    errors.append(m)

        return errors

    def get_data(self):
        return self._o_data

    def check_and_cast(self, prop, value):
        if self._input_properties is not None and prop in self._input_properties:
            prop_schema = self._input_properties[prop]
            _type_str = "type"
            if _type_str in prop_schema:
                parameter_type = prop_schema[_type_str]
                try:
                    if parameter_type == "integer" and not isinstance(value, int):
                        return int(value)
                    if parameter_type == "string" and not isinstance(value, str):
                        return str(value)
                    if parameter_type == "number" and not isinstance(value, float):
                        return float(value)
                except ValueError:
                    pass
        return value


class DataProviderHelper:

    def __init__(self):
        self._cache = {}

    @staticmethod
    def get_executable_content(char, twig, input_shape):
        nulls = []
        if "nullable" in twig:
            for n in twig["nullable"]:
                if input_shape.get_prop(n) is None:
                    nulls.append(n)
        return compile_sql(twig, nulls, char)

    def build_parameters(self, query, input_shape, get_value_converter):
        values = []
        _cache = self._cache

        if "parameters" in query:
            parameters = query["parameters"]
            for p in parameters:
                param_name = p["name"]
                param_type = p["type"]

                if param_name in _cache:
                    param_value = _cache[param_name]
                else:
                    param_value = input_shape.get_prop(param_name)

                    if not ("$params" in param_name or "$parent" in param_name):
                        _cache[param_name] = param_value

                try:
                    if param_value:
                        if param_type == "integer":
                            param_value = int(param_value)
                        elif param_type == "string":
                            param_value = str(param_value)
                        elif param_type == "json":
                            param_value = json.dumps(param_value.get_data())
                        else:
                            if type(param_value) == Shape:
                                param_value = json.dumps(param_value.get_data())
                            else:
                                param_value = get_value_converter(param_type, param_value)
                    values.append(param_value)
                except ValueError:
                    values.append(param_value)

        return values


class FileContentReader:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_sql(self, method, path):
        file_path = path_join(*[self._root_path, path, method + ".sql"])
        return self._get(file_path)

    def get_routes_config(self, path):
        routes_path = path_join(*[self._root_path, path])
        return self._get_config(routes_path)

    def get_config(self, path):
        input_path = path_join(*[self._root_path, path, "$.input"])
        input_config = self._get_config(input_path)

        output_path = path_join(*[self._root_path, path, "$.output"])
        output_config = self._get_config(output_path)

        return {"input.model": input_config, "output.model": output_config}

    def list_sql(self, path):
        try:
            files = os.listdir(path_join(*[self._root_path, path]))
            return [f.replace(".sql", "") for f in files if f.endswith(".sql")]
        except FileNotFoundError:
            return None

    def _get_config(self, file_path):
        yaml_path = file_path + ".yaml"
        if os.path.exists(yaml_path):
            config_str = self._get(yaml_path)
            if config_str is not None and config_str != '':
                return yaml.load(config_str)

        json_path = file_path + ".json"
        if os.path.exists(json_path):
            config_str = self._get(json_path)
            if config_str is not None and config_str != '':
                return json.loads(config_str)

    @staticmethod
    def _get(file_path):
        try:
            # print(file_path)
            with open(file_path, "r") as file:
                content = file.read()
        except FileNotFoundError:
            content = None
        return content


class Yaal:

    def __init__(self, root_path, content_reader, debug):
        self._root_path = root_path
        self._descriptors = {}
        self._data_providers = {}
        self._debug = debug

        if not content_reader:
            self._content_reader = FileContentReader(self._root_path)
        else:
            self._content_reader = content_reader

        self._routes = _build_routes(self._content_reader.get_routes_config("routes"))

    def get_routes(self):
        return self._routes

    def setup_data_provider(self, name, database_uri):
        provider_name, options = _parse_rfc1738_args(database_uri)
        if provider_name == "postgresql":
            self._data_providers[name] = PostgresContextManager(options)
        if provider_name == "mysql":
            self._data_providers[name] = MySQLContextManager(options)
        elif provider_name == "sqlite3":
            self._data_providers[name] = SQLiteContextManager(options)
        return None

    def get_data_provider(self, name):
        return self._data_providers[name].get_context()

    def create_descriptor(self, path):
        return create_trunk(path, self._content_reader)

    def get_descriptor(self, route, path):
        k = path_join(*[route])
        if not self._debug and k in self._descriptors:
            return self._descriptors[k]

        descriptor = self.create_descriptor(path)
        self._descriptors[k] = descriptor
        return descriptor

    def get_descriptor_path_by_route(self, path):
        path_values = {}
        if self._routes:
            for r in self._routes:
                m = r["route"].match(path)
                if m:
                    for k, v in m.groupdict().items():
                        path_values[k] = v
                    return r["descriptor"], r["path"], path_values
        return path, path, None

    def get_result_json(self, descriptor, context):
        return get_result_json(descriptor, self.get_data_provider, context)

    def get_root_path(self):
        return self._root_path


class SQLiteContextManager:

    def __init__(self, options):
        self._options = options

    def get_context(self):
        return SQLiteDataProvider(self._options)


class SQLiteDataProvider:

    def __init__(self, options):
        self._database = options["database"]
        if self._database == "":
            self._database = ":memory:"
        self._con = None

    @staticmethod
    def _sqlite_dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def begin(self):
        self._con = sqlite3.connect(self._database)
        self._con.row_factory = self._sqlite_dict_factory

    def end(self):
        try:
            if self._con:
                self._con.commit()
                self._con.close()
        except Exception as e:
            raise e
        finally:
            self._con = None

    def error(self):
        if self._con:
            self._con.rollback()
            self._con.close()
            self._con = None

    @staticmethod
    def get_value(parameter_type, value):
        if parameter_type == "blob":
            return sqlite3.Binary(value)
        return value

    def execute(self, twig, input_shape, helper):
        con = self._con
        sql = helper.get_executable_content("?", twig, input_shape)
        with con:
            cur = con.cursor()
            args = helper.build_parameters(sql, input_shape, self.get_value)
            print(sql["content"])
            cur.execute(sql["content"], args)
            rows = cur.fetchall()

        return rows, cur.lastrowid
