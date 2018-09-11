import os, json 
import copy
from flask import Flask, request, abort, send_from_directory
from gravity import get_namespace, create_context, get_descriptor_json

app = Flask(__name__)
root_path  = "serve"
path_join = os.path.join

@app.route("/_<namespace>/", methods=["GET"], defaults = { "path" : "" })
@app.route("/_<namespace>/<path:path>", methods=["GET"])
def serve_app(namespace, path):
    static_file_dir = os.path.join(root_path, namespace, "app")
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, "index.html")
 
    return send_from_directory(static_file_dir, path)

@app.route("/_<namespace>/api/", methods=["GET"], defaults = { "path" : "" })
@app.route("/_<namespace>/api/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def namespace_serve_api(namespace, path):

    g = get_namespace(namespace, root_path, True)
    
    method = request.method.lower()
    descriptor_path, route_path, path_values = g.get_descriptor_path_by_route(path)    
    descriptor = g.get_descriptor(path_join(*[route_path, method]), path_join(*[descriptor_path, method]))
    
    if not descriptor:
        return abort(404)   

    if "debug" in request.args:        
        return get_descriptor_json(descriptor)

    ctx = create_gravity_context(request, path_values, namespace, path, descriptor)
    rs = g.get_result_json(descriptor, ctx)
    
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

def create_gravity_context(request, path_values, namespace, path, descriptor):
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

    headers = {}
    for k, v in request.headers.items():
        headers[k] = v

    cookies = {}
    for k, v in request.cookies.items():
        cookies[k] = v

    return create_context(descriptor, path, request_body, params, query, path_values, headers, cookies)    

if __name__ == "__main__":
    app.run(debug=True)
