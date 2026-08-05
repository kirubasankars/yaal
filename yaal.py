# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import copy
import json
import os
import re
import uuid
from urllib.parse import parse_qsl, unquote_plus

import yaml

from yaal_builder import create_trunk
from yaal_errors import (
    DescriptorNotFoundError,
    PathEscapeError,
    UnsupportedDatabaseUrlError,
    YaalError,
)
from yaal_executor import DataProviderHelper, get_result, get_result_json
from yaal_shape import Shape
from yaal_sqlite import SQLiteContextManager

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
    validators = descriptor.get("_validators")
    if model and validators:
        if args_str in model:
            args_schema = model[args_str]
            args_validator = validators.get(args_str)
        else:
            args_schema = None
            args_validator = None

        if payload_str in model:
            payload_schema = model[payload_str]
            payload_validator = validators.get(payload_str)
        else:
            payload_schema = None
            payload_validator = None
    else:
        args_schema = None
        args_validator = None
        payload_schema = None
        payload_validator = None

    args_shape = Shape(schema=args_schema, validator=args_validator)
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

    return Shape(schema=payload_schema, validator=payload_validator, data=payload, extras=extras)


def _normalize_sqlite_options(options):
    """Repair common sqlite3:// URL shapes into a usable filesystem path."""
    options = dict(options)
    database = options.get("database")
    host = options.get("host")
    if database is None:
        database = ""

    if host == ".":
        database = "./" + database if database else "."
    elif host:
        database = host + ("/" + database if database else "")

    options["database"] = database
    options["host"] = None
    return options


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
            query = (len(tokens) > 1 and dict(parse_qsl(tokens[1]))) or None
        else:
            query = None
        components['query'] = query

        if components['username'] is not None:
            components['username'] = unquote_plus(components['username'])
        if components['password'] is not None:
            components['password'] = unquote_plus(components['password'])

        provider_name = components.pop('name')
        if provider_name == "sqlite3":
            components = _normalize_sqlite_options(components)
        return provider_name, components
    else:
        raise ValueError(
            "Could not parse database URL %r. Expected forms like "
            "sqlite3:////abs/path.db, sqlite3://./rel/path.db, "
            "postgresql://user:pass@host:5432/db, mysql://user:pass@host:3306/db, "
            "clickhouse://user:pass@host:9000/db"
            % connection_url
        )


class FileContentReader:

    def __init__(self, root_path):
        self._root_path = os.path.realpath(root_path)

    def get_sql(self, method, path):
        file_path = self._resolve(path, method + ".sql")
        return self._get(file_path)

    def get_config(self, path, output_mapper):
        input_path = self._resolve(path, "$.input")
        input_config = self._get_config(input_path)

        output_name = "$.output" + ("." + output_mapper if output_mapper else "")
        output_path = self._resolve(path, output_name)
        output_config = self._get_config(output_path)

        return {"input.model": input_config, "output.model": output_config}

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
        yaml_path = file_path + ".yaml"
        if os.path.exists(yaml_path):
            config_str = self._get(yaml_path)
            if config_str is not None and config_str != '':
                return yaml.safe_load(config_str)

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
        self._data_providers = {}
        self._data_provider_schemes = {}
        self._debug = debug
        self._precompiled = precompiled

        if not content_reader:
            self._content_reader = FileContentReader(self._root_path)
        else:
            self._content_reader = content_reader

    def setup_data_provider(self, name, database_uri):
        provider_name, options = _parse_rfc1738_args(database_uri)
        if provider_name == "postgresql":
            from yaal_postgres import PostgresContextManager
            self._data_providers[name] = PostgresContextManager(options)
        elif provider_name == "mysql":
            from yaal_mysql import MySQLContextManager
            self._data_providers[name] = MySQLContextManager(options)
        elif provider_name == "clickhouse":
            from yaal_clickhouse import ClickHouseContextManager
            self._data_providers[name] = ClickHouseContextManager(options)
        elif provider_name == "sqlite3":
            self._data_providers[name] = SQLiteContextManager(options)
        else:
            raise UnsupportedDatabaseUrlError(
                "Unsupported database URL scheme %r for provider %r. "
                "Supported schemes: sqlite3, postgresql, mysql, clickhouse"
                % (provider_name, name)
            )
        self._data_provider_schemes[name] = provider_name
        return None

    def get_data_provider(self, name):
        if name not in self._data_providers:
            raise YaalError(
                "Data provider %r is not configured. Call setup_data_provider(%r, url) first."
                % (name, name)
            )
        return self._data_providers[name].get_context()

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
        """Clear cached descriptors (reload SQL/YAML on next query)."""
        self._descriptors = {}

    def _descriptor_key(self, descriptor_path, output_mapper=None):
        if output_mapper:
            return descriptor_path + "#" + output_mapper
        return descriptor_path

    def _load_descriptor(self, descriptor_path, output_mapper=None):
        cache_key = self._descriptor_key(descriptor_path, output_mapper)
        if not self._debug and cache_key in self._descriptors:
            return self._descriptors[cache_key]

        # debug=True forces live SQL/YAML; otherwise prefer precompiled artifacts.
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

    def _default_placeholder(self):
        for scheme in self._data_provider_schemes.values():
            if scheme in ("postgresql", "mysql", "clickhouse"):
                return "%s"
            if scheme == "sqlite3":
                return "?"
        return "?"

    def query(self, descriptor_path, *, payload=None, args=None, output_mapper=None):
        """Load a descriptor, build context, and return the SQL→JSON result."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        return self.get_result(descriptor, context)

    def query_json(self, descriptor_path, *, payload=None, args=None, output_mapper=None):
        """Same as query, but return a JSON string."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        return self.get_result_json(descriptor, context)

    def explain_sql(self, descriptor_path, *, payload=None, args=None,
                    output_mapper=None, placeholder=None):
        """Return compiled SQL twigs after null-filter elision (for authoring/debug)."""
        descriptor = self._load_descriptor(descriptor_path, output_mapper)
        context = create_context(descriptor, payload=payload, args=args)
        if placeholder is None:
            placeholder = self._default_placeholder()

        helper = DataProviderHelper()
        explained = []

        def _identity_converter(_param_type, param_value):
            return param_value

        def walk(branch, shape):
            for twig in branch.get("twigs") or []:
                compiled = helper.get_executable_content(placeholder, twig, shape)
                explained.append({
                    "method": branch.get("method"),
                    "connection": twig.get("connection", "db"),
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

    def get_result(self, descriptor, context):
        return get_result(descriptor, self.get_data_provider, context)

    def get_result_json(self, descriptor, context):
        return get_result_json(descriptor, self.get_data_provider, context)

    def get_root_path(self):
        return self._root_path
