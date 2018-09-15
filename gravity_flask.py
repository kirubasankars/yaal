from gravity import create_context


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


def create_gravity_context(req, path_values, app, path, descriptor):
    if req.mimetype == "application/json":
        try:
            request_body = req.get_json()
        except Exception:
            request_body = None
    else:
        request_body = None

    if req.mimetype == "multipart/form-data":
        request_body = request_body or {}
        for k, v in req.form.items():
            request_body[k.lower()] = v

    query = {}
    for k, v in req.args.items():
        query[k] = v

    headers = {}
    for k, v in req.headers.items():
        headers[k] = v

    cookies = {}
    for k, v in req.cookies.items():
        cookies[k] = v

    return create_context(descriptor, app, path, request_body, query, path_values, headers, cookies)
