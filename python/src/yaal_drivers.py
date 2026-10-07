# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""URL providers for the CLI, the demo, and tests. The Yaal library does not import this module."""

import re
from urllib.parse import parse_qsl, unquote_plus

from yaal_errors import UnsupportedDatabaseUrlError, YaalError


def open(database_uri):
    """Open one provider for a database URL. The caller passes it to query."""
    provider_name, options = _parse_rfc1738_args(database_uri)
    if provider_name == "postgresql":
        try:
            from yaal_postgres import PostgresContextManager
        except ImportError as e:
            raise YaalError(
                "PostgreSQL requires psycopg2. pip install 'yaal[postgres]'"
            ) from e
        manager = PostgresContextManager(options)
    elif provider_name == "mysql":
        try:
            from yaal_mysql import MySQLContextManager
        except ImportError as e:
            raise YaalError(
                "MySQL requires mysql-connector-python. pip install 'yaal[mysql]'"
            ) from e
        manager = MySQLContextManager(options)
    elif provider_name == "clickhouse":
        try:
            from yaal_clickhouse import ClickHouseContextManager
        except ImportError as e:
            raise YaalError(
                "ClickHouse requires clickhouse-driver. pip install 'yaal[clickhouse]'"
            ) from e
        manager = ClickHouseContextManager(options)
    elif provider_name == "sqlite3":
        from yaal_sqlite import SQLiteContextManager
        manager = SQLiteContextManager(options)
    else:
        raise UnsupportedDatabaseUrlError(
            "Unsupported database URL scheme %r. "
            "Supported schemes: sqlite3, postgresql, mysql, clickhouse"
            % provider_name
        )
    return manager.get_context()


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
    raise ValueError(
        "Could not parse database URL %r. Expected forms like "
        "sqlite3:////abs/path.db, sqlite3://./rel/path.db, "
        "postgresql://user:pass@host:5432/db, mysql://user:pass@host:3306/db, "
        "clickhouse://user:pass@host:9000/db"
        % connection_url
    )
