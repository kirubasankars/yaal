import os
from flask import Flask, request, abort, send_from_directory
from yaal import Yaal, get_descriptor_json
from yaal_flask import create_yaal_context, create_flask_response

path_join = os.path.join

DATA_PROVIDERS= {
    "db": "sqlite3:///"
}
YAAL_DEBUG = False
CORS_ENABLED = False

app = Flask(__name__)
root_path = "serve"
app.config.from_object(__name__)
app.config.from_pyfile(path_join(*[root_path, "config.cfg"]))

y = Yaal(path_join(*[root_path, "api"]), None, app.config["YAAL_DEBUG"])
for name, db_url in app.config["DATA_PROVIDERS"].items():
    y.setup_data_provider(name, db_url)


@app.route("/", methods=["GET"], defaults={"path": ""})
@app.route("/<path:path>", methods=["GET"])
def serve_app(path):
    static_file_dir = os.path.join(root_path, "app")
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, "index.html")

    return send_from_directory(static_file_dir, path)


@app.route("/api/", methods=["GET"], defaults={"path": ""})
@app.route("/api/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
def namespace_serve_api(path):
    method = request.method.lower()

    descriptor_path, route_path, path_values = y.get_descriptor_path_by_route(path)
    descriptor = y.get_descriptor(path_join(*[route_path, method]), path_join(*[descriptor_path, method]))

    if not descriptor:
        return abort(404)

    if "debug" in request.args and app.config["YAAL_DEBUG"]:
        return app.response_class(get_descriptor_json(descriptor), content_type="application/json")

    ctx = create_yaal_context(request, path_values, descriptor)
    rs = y.get_result_json(descriptor, ctx)

    return create_flask_response(app, ctx, rs)


if __name__ == "__main__":
    app.run(debug=app.config["YAAL_DEBUG"])
