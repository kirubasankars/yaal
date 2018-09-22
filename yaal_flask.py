from yaal import create_context


def create_flask_response(flask_app, context, result):
    r = context.get_prop("$response")
    header = r.get_prop("$header")
    cookie = r.get_prop("$cookie")

    resp = flask_app.response_class(result, content_type="application/json", status=r.get_prop("status_code"))

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

    if req.mimetype == "multipart/form-data":
        payload = payload or {}
        for k, v in req.form.items():
            payload[k.lower()] = v

    query = {}
    for k, v in req.args.items():
        query[k] = v

    headers = {}
    for k, v in req.headers.items():
        headers[k] = v

    cookies = {}
    for k, v in req.cookies.items():
        cookies[k] = v

    return create_context(descriptor, payload, query, path_values, headers, cookies)
