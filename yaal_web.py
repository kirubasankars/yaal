import os
from flask import Flask, request, abort, send_from_directory
from yaal import Yaal, get_descriptor_json
from yaal_flask import create_gravity_context, create_flask_response

app = Flask(__name__)

root_path = "serve/api"
g = Yaal(root_path, None, False)
g.setup_data_provider("postgresql://postgres:admin@localhost/dvdrental")

path_join = os.path.join


@app.route("/", methods=["GET"], defaults={"path": ""})
@app.route("/<path:path>", methods=["GET"])
def serve_app(path):
    static_file_dir = os.path.join(root_path, "app")
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, "index.html")
 
    return send_from_directory(static_file_dir, path)


@app.route("/api/", methods=["GET"], defaults={"path": ""})
@app.route("/api/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def namespace_serve_api(path):

    method = request.method.lower()
    descriptor_path, route_path, path_values = g.get_descriptor_path_by_route(path)
    descriptor = g.get_descriptor(path_join(*[route_path, method]), path_join(*[descriptor_path, method]))
    
    if not descriptor:
        return abort(404)   

    if "debug" in request.args:        
        return get_descriptor_json(descriptor)

    ctx = create_gravity_context(request, path_values, "", path, descriptor)
    rs = g.get_result_json(descriptor, ctx)

    return create_flask_response(app, ctx, rs)


if __name__ == "__main__":
    app.run(debug=False)
