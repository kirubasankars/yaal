# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import copy
import json
import os
import uuid

from yaal_builder import create_trunk
from yaal_errors import (
    DescriptorNotFoundError,
    PathEscapeError,
)
from yaal_executor import DataProviderHelper, get_result, get_result_json
from yaal_shape import Shape

path_join = os.path.join


def _strip_descriptor_for_json(descriptor, pretty=False):
    if "_validators" in descriptor:
        del descriptor["_validators"]
    if "branches" in descriptor:
        for branch in descriptor["branches"]:
            _strip_descriptor_for_json(branch, pretty)
    if "twigs" in descriptor:
        for twig in descriptor["twigs"]:
            content = twig["content"]

            if pretty:
                content_length = len(content)
                i = 0

                while True:
                    if i >= content_length:
                        break

                    item = content[i]

                    if item["type"] == "newline":
                        item["value"] = " "
                    if item["type"] == "space":
                        item["value"] = " "

                    if i + 1 < content_length:
                        item1 = content[i + 1]
                        if (item["type"] == "newline" or item["type"] == "space") \
                                and (item1["type"] == "space" or item1["type"] == "newline"):
                            item["value"] = ""

                    i = i + 1

            twig["content"] = "".join([x["value"] for x in twig["content"]]).lstrip().rstrip()


# Back-compat alias
debug_descriptor = _strip_descriptor_for_json


def get_descriptor_json(descriptor, pretty=False):
    d = copy.deepcopy(descriptor)
    _strip_descriptor_for_json(d, pretty)
    if pretty:
        return json.dumps(d, indent=4)
    else:
        return json.dumps(d)


def create_context(descriptor, payload=None, args=None):
    args_str, payload_str = "args", "payload"

    model = descriptor.get("model")
    if model:
        args_schema = model.get(args_str)
        payload_schema = model.get(payload_str)
    else:
        args_schema = None
        payload_schema = None

    args_shape = Shape(schema=args_schema)
    if args:
        for k, v in args.items():
            args_shape.set_prop(k, v)

    params_shape = Shape(data={
        "path": descriptor["path"],
        "$run_id": str(uuid.uuid4()),
    })

    extras = {
        "$params": params_shape,
        "$args": args_shape,
    }

    return Shape(schema=payload_schema, data=payload, extras=extras)


class FileContentReader:

    def __init__(self, root_path):
        self._root_path = os.path.realpath(root_path)

    def get_sql(self, method, path):
        file_path = self._resolve(path, method + ".sql")
        return self._get(file_path)

    def get_config(self, path, output_mapper):
        output_name = "$.output" + ("." + output_mapper if output_mapper else "")
        output_path = self._resolve(path, output_name)
        output_config = self._get_config(output_path)

        return {"output.model": output_config}

    def list_sql(self, path):
        try:
            files = os.listdir(self._resolve(path))
            return [f.replace(".sql", "") for f in files if f.endswith(".sql")]
        except FileNotFoundError:
            return None

    def _resolve(self, *parts):
        """Join under root and reject paths that escape the API tree."""
        candidate = os.path.realpath(path_join(self._root_path, *parts))
        root = self._root_path
        if candidate == root or candidate.startswith(root + os.sep):
            return candidate
        raise PathEscapeError(
            "descriptor path %r resolves outside API root %r" % (parts, root)
        )

    def _get_config(self, file_path):
        json_path = file_path + ".json"
        if os.path.exists(json_path):
            config_str = self._get(json_path)
            if config_str is not None and config_str != '':
                return json.loads(config_str)

    @staticmethod
    def _get(file_path):
        try:
            with open(file_path, "r") as file:
                content = file.read()
        except FileNotFoundError:
            content = None
        return content


class Yaal:

    def __init__(self, root_path, content_reader=None, *, debug=False, precompiled=None):
        self._root_path = root_path
        self._descriptors = {}
        self._debug = debug
        self._precompiled = precompiled

        if not content_reader:
            self._content_reader = FileContentReader(self._root_path)
        else:
            self._content_reader = content_reader

    def create_descriptor(self, path, output_mapper=None):
        descriptor = create_trunk(path, output_mapper, self._content_reader)
        if descriptor is None:
            root = getattr(self._content_reader, "_root_path", self._root_path)
            raise DescriptorNotFoundError(
                "No SQL descriptor files (*.sql) found at %s"
                % path_join(root, path)
            )
        return descriptor

    def clear_cache(self):
        """Clear cached descriptors (reload SQL/JSON on next query)."""
        self._descriptors = {}

    def _descriptor_key(self, descriptor_path, output_mapper=None):
        if output_mapper:
            return descriptor_path + "#" + output_mapper
        return descriptor_path

    def _load_descriptor(self, descriptor_path, output_mapper=None):
        cache_key = self._descriptor_key(descriptor_path, output_mapper)
        if not self._debug and cache_key in self._descriptors:
            return self._descriptors[cache_key]

        # debug=True forces live SQL/JSON; otherwise prefer precompiled artifacts.
        if self._precompiled and not self._debug:
            descriptor = self._load_precompiled(descriptor_path, output_mapper)
        else:
            descriptor = self.create_descriptor(descriptor_path, output_mapper)
        self._descriptors[cache_key] = descriptor
        return descriptor

    def _load_precompiled(self, descriptor_path, output_mapper=None):
        from yaal_precompile import load_precompiled_file, resolve_precompiled_path

        file_path = resolve_precompiled_path(
            self._precompiled, descriptor_path, output_mapper
        )
        if not os.path.isfile(file_path):
            raise DescriptorNotFoundError(
                "No precompiled descriptor at %s" % file_path
            )
        return load_precompiled_file(file_path)

    def query(self, provider, descriptor_path, *, payload=None, args=None, output_mapper=None):
        """Load a descriptor, build context, and return the SQL→JSON result."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        return self.get_result(provider, descriptor, context)

    def query_json(self, provider, descriptor_path, *, payload=None, args=None, output_mapper=None):
        """Same as query, but return a JSON string."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        return self.get_result_json(provider, descriptor, context)

    def explain_sql(self, provider, descriptor_path, *, payload=None, args=None,
                    output_mapper=None, placeholder=None):
        """Return compiled SQL twigs after null-filter elision (for authoring/debug)."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        if placeholder is None:
            placeholder = getattr(provider, "placeholder", None) or "?"

        helper = DataProviderHelper()
        explained = []

        def _identity_converter(_param_type, param_value):
            return param_value

        def walk(branch, shape):
            for twig in branch.get("twigs") or []:
                compiled = helper.get_executable_content(placeholder, twig, shape)
                explained.append({
                    "method": branch.get("method"),
                    "sql": compiled["content"],
                    "parameters": helper.build_parameters(
                        compiled, shape, _identity_converter
                    ),
                })
            for child in branch.get("branches") or []:
                child_shape = shape
                child_name = (child.get("name") or "").lower()
                if child_name:
                    nested = shape.get_prop(child_name)
                    if nested is not None:
                        child_shape = nested
                walk(child, child_shape)

        walk(descriptor, context)
        return explained

    def get_result(self, provider, descriptor, context):
        return get_result(descriptor, provider, context)

    def get_result_json(self, provider, descriptor, context):
        return get_result_json(descriptor, provider, context)

    def get_root_path(self):
        return self._root_path
