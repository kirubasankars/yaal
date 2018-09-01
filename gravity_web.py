import os, json 
import copy
from flask import Flask, request, abort, send_from_directory
from gravity import get_namespace, create_context, get_result_json, get_descriptor_json

app = Flask(__name__)
root_path  = "serve"

@app.route("/", methods=["GET"], defaults = { "path" : "" })
@app.route("/<path:path>", methods=["GET"])
def serve_app(path):
    namespace = ""
    static_file_dir = os.path.join(root_path, namespace, "app")
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, "index.html")
 
    return send_from_directory(static_file_dir, path)


@app.route("/api/", methods=["GET"], defaults = { "path" : "" })
@app.route("/api/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def namespace_serve_api(path):
    
    namespace = ""
    gravity_app = get_namespace(namespace, root_path, True)
    
    method = request.method.lower()    
    descriptor = gravity_app.get_descriptor(method, path)
    
    if not descriptor:
        return abort(404)   

    if "debug" in request.args:        
        return get_descriptor_json(descriptor)

    ctx = create_gravity_context(request, namespace, path, descriptor)

    data_providers = gravity_app.get_data_providers()
    rs = get_result_json(descriptor, data_providers, ctx)
    
    r = ctx.get_prop("$response")
    header = r.get_prop("$header")
    cookie = r.get_prop("$cookie")

    resp = app.response_class(rs, content_type="application/json", status=r.get_prop("status_code"))

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


def create_gravity_context(request, namespace, path, node_descriptor):
    if request.mimetype == "application/json":
        try:
            request_body = request.get_json()
        except:
            request_body = None
    else:
        request_body = None

    if request.mimetype == "multipart/form-data":
        request_body = request_body or {}
        for k, v in request.form.items():
            request_body[k] = v

    params = {
        "namespace": namespace,
        "path": path         
    }

    query = {}
    for k,v in request.args.items():
        query[k] = v 

    return create_context(node_descriptor, request_body, params, query, request.headers, request.cookies)    


if __name__ == "__main__":
    app.run(debug=False)

