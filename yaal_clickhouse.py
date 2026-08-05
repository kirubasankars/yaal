from clickhouse_driver import Client

from yaal_provider import fetch_mapped_rows


_CONNECT_QUERY_KEYS = (
    "secure",
    "verify",
    "connect_timeout",
    "send_receive_timeout",
    "sync_request_timeout",
    "compress_block_size",
)


def _coerce_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes", "on")


def _to_pyformat(sql_content, args):
    """Convert Yaal's positional %s binds to clickhouse-driver pyformat %(pN)s."""
    parts = sql_content.split("%s")
    if len(parts) == 1:
        return sql_content, {}
    if len(parts) - 1 != len(args):
        raise ValueError(
            "ClickHouse bind count mismatch: %d placeholders, %d values"
            % (len(parts) - 1, len(args))
        )
    rendered_parts = [parts[0]]
    params = {}
    for i, part in enumerate(parts[1:]):
        key = "p%d" % i
        params[key] = args[i]
        rendered_parts.append("%(" + key + ")s")
        rendered_parts.append(part)
    return "".join(rendered_parts), params


class ClickHouseContextManager:

    def __init__(self, options):
        port = options.get("port")
        kwargs = {
            "host": options.get("host") or "127.0.0.1",
            "port": int(port) if port else 9000,
            "user": options.get("username") or "default",
            "password": options.get("password") or "",
            "database": options.get("database") or "default",
        }
        query = options.get("query") or {}
        for key in _CONNECT_QUERY_KEYS:
            if key not in query:
                continue
            value = query[key]
            if key in ("secure", "verify"):
                value = _coerce_bool(value)
            elif key.endswith("timeout") or key == "compress_block_size":
                value = int(value)
            kwargs[key] = value
        self._client_kwargs = kwargs

    def get_context(self):
        return ClickHouseDataProvider(self._client_kwargs)


class ClickHouseDataProvider:

    def __init__(self, client_kwargs):
        self._client_kwargs = client_kwargs
        self._client = None

    def begin(self):
        self._client = Client(**self._client_kwargs)

    def end(self):
        client = self._client
        self._client = None
        if client is not None:
            client.disconnect()

    def error(self):
        client = self._client
        self._client = None
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass

    @staticmethod
    def get_value_converter(param_type, value):
        return value

    def execute(self, twig, input_shape, helper):
        client = self._client
        sql = helper.get_executable_content("%s", twig, input_shape)
        args = helper.build_parameters(sql, input_shape, self.get_value_converter)
        content, params = _to_pyformat(sql["content"], args)
        rows_raw, columns_with_types = client.execute(
            content,
            params or None,
            with_column_types=True,
        )
        # ClickHouse qualifies ambiguous names (e.g. u.user_id); Yaal expects bare names.
        column_names = [
            name.rsplit(".", 1)[-1] for name, _type in columns_with_types
        ]
        rows = fetch_mapped_rows(rows_raw, column_names)
        return rows, None
