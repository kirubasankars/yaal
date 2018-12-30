from yaal import create_context


def create_flask_response(flask_app, flask_req, context, result):
    r = context.get_prop("$response")
    header = r.get_prop("$header")
    cookie = r.get_prop("$cookie")

    content_type = "application/json"
    if flask_req.content_type == "application/xml":
        content_type = flask_req.content_type

    resp = flask_app.response_class(result, content_type=content_type, status=r.get_prop("status_code"))

    d = header.get_data()
    for k, v in d.items():
        resp.headers[k] = v["value"]

    d = cookie.get_data()
    for k, v in d.items():
        if "expires" in v:
            expires = v["expires"]
        else:
            expires = None
        if "path" in v:
            p = v["path"]
        else:
            p = None
        resp.set_cookie(k, v["value"], expires=expires, path=p)

    return resp


def create_yaal_context(req, path_values, descriptor):
    if req.mimetype == "application/json":
        try:
            payload = req.get_json()
        except Exception:
            payload = None
    else:
        payload = None

    if req.mimetype == "multipart/form-data" or req.mimetype == "application/x-www-form-urlencoded":
        ke = KeyExpander()
        for k, v in req.form.items():
            ke.set_prop(k, v)
        payload = ke.get_data()

    ke = KeyExpander()
    for k, v in req.args.items():
        ke.set_prop(k, v)
    query = ke.get_data()

    ke = KeyExpander()
    for k, v in req.headers.items():
        ke.set_prop(k, v)
    headers = ke.get_data()

    ke = KeyExpander()
    for k, v in req.cookies.items():
        ke.set_prop(k, v)
    cookies = ke.get_data()

    return create_context(descriptor, payload, query, path_values, headers, cookies)


class KeyExpander:

    def __init__(self, data = {}, key = None, parent = None):
        self._type = None
        self._data = data
        self._key = key
        self._parent = parent
        self._metal = {}

    def set_prop(self, key, value):

        data = self._data
        metal = self._metal
        dot = key.find(".")

        if dot > -1:

            path = key[:dot]
            remaining_path = key[dot + 1:]

            if path[0] == "$":
                if self._type == "object":
                    raise ValueError("expected as object path, given array index")

                try:
                    idx = int(path[1:])
                except:
                    raise KeyError("array path expected as $index.")

                if not isinstance(idx, int):
                    raise KeyError("key expected as array index")

                if self._type is None and len(data) == 0:
                    self._type = "array"

            else:
                if self._type == "array":
                    raise ValueError("expected as array index, given object path")

                if self._type is None:
                    self._type = "object"

            if path not in metal:
                data[path] = {}
                metal[path] = KeyExpander(data[path], path, self)

            return metal[path].set_prop(remaining_path, value)
        else:
            if key[0] == "$":
                if self._type == "object":
                    raise ValueError("expected as object path, given array index")

                try:
                    idx = int(key[1:])
                except:
                    raise KeyError("array path expected as $index.")

                if not isinstance(idx, int):
                    raise KeyError("key expected as array index")

                if self._type is None and len(data) == 0:
                    self._type = "array"
            else:
                if self._type == "array":
                    raise ValueError("expected as array index, given object path")

                if self._type is None:
                    self._type = "object"

            data[key] = value

    def fix_array(self):
        metal = self._metal
        data = []
        for k, v in metal.items():
            v.fix_array()

        if self._type == "array":
            items = sorted([int(item[1:]) for item in self._data])
            for k in items:
                v = self._data["$" + str(k)]
                data.append(v)
            self._parent._data[self._key] = data

        self._parent = None



    def get_data(self):
        self.fix_array()
        return self._data
