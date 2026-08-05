# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import os
import re

from jsonschema import FormatChecker, Draft4Validator

from yaal_parser import lexer, parser
from yaal_shape import _to_lower_keys_deep, _to_lower_keys

path_join = os.path.join


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

        ast = parser(lexer(content), method)
        if "sql_stmts" not in ast:
            return

        branch["parameters"] = ast.get("parameters") or {}

        for k, v in branch["parameters"].items():
            if k[0] == "$" and k.find("$parent") == -1:
                _expand_parameter(model, k, v)
            else:
                _expand_parameter(payload_model, k, v)

        branch["twigs"] = ast["sql_stmts"]

        connections = bag.setdefault("connections", ["db"])
        for twig in branch["twigs"]:
            if not twig.get("connection"):
                twig["connection"] = "db"
            if twig["connection"] not in connections:
                connections.append(twig["connection"])

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

        if _use_parent_rows_str in sub_branch and sub_branch[_use_parent_rows_str]:
            if _partition_by_str not in branch or not branch[_partition_by_str]:
                raise Exception("parent's _partition_by is can't be empty when child wanted to use parent rows")

        branches.append(sub_branch)

    if branches:
        branch["branches"] = branches


array_rx = re.compile(r"^(?P<path>\w+)\[\d+\]$")


def _expand_parameter(model, prop, value):
    dot = prop.find(".")
    if dot > -1:
        path = prop[:dot]
        if path == "$parent":
            if "$parent" in model:
                model = model["$parent"]
            else:
                model = None
        elif path == "$args":
            model = model["args"]
        elif path == "$params":
            return
        else:
            if "properties" not in model:
                model["properties"] = {}

            m = array_rx.search(path)
            if m:
                path = m.groupdict()["path"]
                if path not in model["properties"]:
                    model["properties"][path] = {
                        "type": "array",
                        "properties": {}
                    }
            else:
                if path not in model["properties"]:
                    model["properties"][path] = {
                        "type": "object",
                        "properties": {}
                    }

            model = model["properties"][path]

            if "required" in model:
                for f in model["required"]:
                    if f in model["properties"]:
                        model["properties"][f]["required"] = True

        _expand_parameter(model, prop[dot + 1:], value)
    else:
        if model and "properties" in model:
            if model and prop not in model["properties"]:
                model["properties"][prop] = value


def create_trunk(path, output_mapper, content_reader):
    ordered_files = _order_list_by_dots(content_reader.list_sql(path))
    if len(ordered_files) == 0:
        return None

    trunk_map = _build_trunk_map_by_files(ordered_files)
    config = content_reader.get_config(path, output_mapper)

    input_model_str, output_model_str = "input.model", "output.model"
    args_str, payload_str = "args", "payload"
    payload_schema, args_schema, output_schema = None, None, None

    if config:
        input_model = config.get(input_model_str)
        if input_model:
            payload_schema = _to_lower_keys_deep(input_model.get(payload_str))
            args_schema = _to_lower_keys_deep(input_model.get(args_str))

        output_schema = config.get(output_model_str)

    if not args_schema:
        args_schema = {
            "type": "object",
            "properties": {}
        }
    if not payload_schema:
        payload_schema = {
            "type": "object",
            "properties": {}
        }
    if not output_schema:
        output_schema = {
            "type": "array",
            "properties": {}
        }

    trunk = {
        "name": "$",
        "method": "$",
        "path": path,
        "model": {
            "args": args_schema,
            "payload": payload_schema,
            "output": output_schema
        }
    }

    if payload_schema:
        payload_validator = Draft4Validator(schema=payload_schema, format_checker=FormatChecker())
    else:
        payload_validator = None

    if args_schema:
        args_validator = Draft4Validator(schema=args_schema, format_checker=FormatChecker())
    else:
        args_validator = None

    bag = {"connections": ["db"]}
    _build_branch(trunk, trunk_map["$"], content_reader, payload_schema, output_schema, trunk["model"], bag)
    trunk["connections"] = bag["connections"]

    validators = {
        "args": args_validator,
        "payload": payload_validator
    }
    trunk["_validators"] = validators

    return trunk
