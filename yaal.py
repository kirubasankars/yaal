import copy
import json
import logging
import os
import re
import urllib
import uuid

import yaml

from yaal_builder import create_trunk
from yaal_executor import get_result_json
from yaal_mysql import MySQLContextManager
from yaal_postgres import PostgresContextManager
from yaal_shape import Shape
from yaal_sqlite import SQLiteContextManager

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')
logger = logging

path_join = os.path.join


def debug_descriptor(descriptor, pretty=False):
    if "_validators" in descriptor:
        del descriptor["_validators"]
    if "branches" in descriptor:
        for branch in descriptor["branches"]:
            debug_descriptor(branch, pretty)
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

                    if i + 1 < content_length:
                        item1 = content[i + 1]
                    if item["type"] == "newline":
                        item["value"] = " "
                    if item["type"] == "space":
                        item["value"] = " "

                    if (item["type"] == "newline" or item["type"] == "space") \
                            and (item1["type"] == "space" or item1["type"] == "newline"):
                        item["value"] = ""

                    i = i + 1

            twig["content"] = "".join([x["value"] for x in twig["content"]]).lstrip().rstrip()


def get_descriptor_json(descriptor, pretty=False):
    d = copy.deepcopy(descriptor)
    debug_descriptor(d, pretty)
    if pretty:
        return json.dumps(d, indent=4)
    else:
        return json.dumps(d)


def create_context(descriptor, payload=None, query=None, path_values=None, header=None, cookie=None):
    query_str, path_str, header_str, cookie_str, payload_str = "query", "path", "header", "cookie", "payload"

    model = descriptor.get("model")
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

    query_shape = Shape(schema=query_schema, validator=query_validator)
    if query:
        for k, v in query.items():
            query_shape.set_prop(k, v)

    path_shape = Shape(schema=path_schema, validator=path_validator)
    if path_values:
        for k, v in path_values.items():
            path_shape.set_prop(k, v)

    header_shape = Shape(schema=header_schema, validator=header_validator, data=header)
    cookie_shape = Shape(schema=cookie_schema, validator=cookie_validator, data=cookie)

    request_extras = {
        "$query": query_shape,
        "$path": path_shape,
        "$header": header_shape,
        "$cookie": cookie_shape
    }

    request_data = {"id": str(uuid.uuid4()) }
    request_shape = Shape(data=request_data, extras=request_extras)

    response_extras = {
        "$header": Shape(),
        "$cookie": Shape()
    }
    response_shape = Shape(extras=response_extras)

    vars = {
        "path": descriptor["path"]
    }
    vars_shape = Shape(data=vars)

    extras = {
        "$params": vars_shape,
        "$query": query_shape,
        "$path": path_shape,
        "$header": header_shape,
        "$cookie": cookie_shape,
        "$request": request_shape,
        "$response": response_shape
    }

    return Shape(schema=payload_schema, validator=payload_validator, data=payload, extras=extras)


def _build_routes(routes):
    _routes = []
    if routes:
        for r in routes:
            if "route" in r and "descriptor" in r:
                p = "^" + re.sub(r"{(.*?)}", r"(?P<\1>[^/]+)", r["route"]) + "/?$"
                _routes.append({
                    "route": re.compile(p),
                    "descriptor": r["descriptor"],
                    "path": r["route"],
                    "output_mapper": r["mapper"] if "mapper" in r else None
                })
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


class FileContentReader:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_sql(self, method, path):
        file_path = path_join(*[self._root_path, path, method + ".sql"])
        return self._get(file_path)

    def get_routes_config(self, path):
        routes_path = path_join(*[self._root_path, path])
        return self._get_config(routes_path)

    def get_config(self, path, output_mapper):
        input_path = path_join(*[self._root_path, path, "$.input"])
        input_config = self._get_config(input_path)

        output_path = path_join(*[self._root_path, path, "$.output" + ("." + output_mapper if output_mapper else "")])
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
        self._cache = {}
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

    def create_descriptor(self, path, output_mapper):
        return create_trunk(path, output_mapper, self._content_reader)

    def clear_cache(self):
        self._descriptors = {}
        self._cache = {}
        self._routes = _build_routes(self._content_reader.get_routes_config("routes"))

    def get_descriptor(self, descriptor_ctx):
        path = descriptor_ctx["path"]
        if not self._debug and path in self._descriptors:
            return self._descriptors[path]

        descriptor_path, route_path = descriptor_ctx["descriptor_path"], descriptor_ctx["route_path"]
        output_mapper = descriptor_ctx["output_mapper"]
        descriptor = self.create_descriptor(descriptor_path, output_mapper)
        self._descriptors[path] = descriptor
        return descriptor

    def get_descriptor_path_by_route(self, path, method):
        path_values, descriptor_path, route_path, output_mapper = None, None, None, None

        for r in self._routes:
            m = r["route"].match(path)
            if m:
                path_values = {}
                for k, v in m.groupdict().items():
                    path_values[k] = v
                descriptor_path = path_join(*[r["descriptor"], method])
                route_path = r["path"]
                output_mapper = r["output_mapper"]
                break

        if not descriptor_path:
            descriptor_path = path_join(*[path, method])
            path_values = None

        return {
            "descriptor_path": descriptor_path,
            "route_path": route_path,
            "path_values": path_values,
            "output_mapper": output_mapper,
            "path": route_path if route_path else descriptor_path
        }

    def get_result_json(self, descriptor, descriptor_ctx, context):

        path = descriptor_ctx["path"]
        if path in self._cache:
            cache_provider = self._cache[path]
        else:
            cache_provider = {}
            self._cache[path] = cache_provider

        return get_result_json(descriptor, self.get_data_provider, context, cache_provider)

    def get_root_path(self):
        return self._root_path
