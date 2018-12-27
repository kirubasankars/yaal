import json
import os
import re

from flask import Flask, request, abort, send_from_directory

from yaal import Yaal, get_descriptor_json
from yaal_flask import create_yaal_context, create_flask_response
from yaal_openapi import build_api_meta

path_join = os.path.join

YAAL_DEBUG = False

DATA_PROVIDERS = {
    "db": "sqlite3:///"
}

CORS_ENABLED = False

app = Flask(__name__)
root_path = "serve"
app.config.from_object(__name__)
app.config.from_pyfile(path_join(*[root_path, "config.cfg"]))

y = Yaal(path_join(*[root_path, "api"]), None, app.config["YAAL_DEBUG"])
for name, db_url in app.config["DATA_PROVIDERS"].items():
    y.setup_data_provider(name, db_url)


@app.route("/_health", methods=["GET"])
def health_api():
    return ""


@app.route("/_debug", methods=["GET"])
def debug_api():
    if "route" in request.args and "method" in request.args:
        r = re.sub("^/api/", "", request.args["route"])
        m = request.args["method"]
        pretty = False
        if "pretty" in request.args:
            pretty = True
        descriptor_ctx = y.get_descriptor_path_by_route(r, m)
        descriptor = y.create_descriptor(descriptor_ctx["descriptor_path"], descriptor_ctx["output_mapper"])
        if descriptor:
            return app.response_class(get_descriptor_json(descriptor, pretty), content_type="application/json")
        else:
            return abort(404)
    else:
        return abort(404)


@app.route("/_clear/cache", methods=["POST"])
def clear_cache():
    y.clear_cache()
    return app.response_class("{\"ok\": true}", content_type="application/json")


@app.route("/_openapi")
def swagger_meta():
    paths = build_api_meta(y)
    res = {"openapi": "3.0.0", "paths": paths}
    return app.response_class(json.dumps(res, indent=4), content_type="application/json")


@app.route("/", methods=["GET"], defaults={"path": ""})
@app.route("/<path:path>", methods=["GET"])
def serve_app(path):
    static_file_dir = os.path.join(root_path, "app")
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, "index.html")

    return send_from_directory(static_file_dir, path)


@app.route("/api/", methods=["GET"], defaults={"path": ""})
@app.route("/api/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"])
def namespace_serve_api(path):
    method = request.method.lower()

    descriptor_ctx = y.get_descriptor_path_by_route(path, method)
    descriptor = y.get_descriptor(descriptor_ctx)

    if not descriptor:
        return abort(404)

    ctx = create_yaal_context(request, descriptor_ctx["path_values"], descriptor)

    rs = y.get_result_json(descriptor, descriptor_ctx, ctx)

    return create_flask_response(app, ctx, rs)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=app.config["YAAL_DEBUG"])
