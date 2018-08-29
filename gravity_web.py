import os, json 
import copy
from flask import Flask, request, abort, send_from_directory
from gravity import get_namespace, create_context, get_result_json

app = Flask(__name__)
namespaces = {}


@app.route('/<namespace>', methods=['GET'])
def namespace_root(namespace):
    static_file_dir = os.path.join('serve', namespace, 'app')
    return send_from_directory(static_file_dir, 'index.html')
 

@app.route('/<namespace>/<path:path>', methods=['GET'])
def serve_app(namespace, path):
    static_file_dir = os.path.join('serve', namespace, 'app')
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, 'index.html')
 
    return send_from_directory(static_file_dir, path)


@app.route('/<namespace>/api/', methods=['GET'], defaults = { 'path' : '' })
@app.route('/<namespace>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def namespace_serve_api(namespace, path):
    
    gravity_app = get_namespace(namespace, "serve", True)
    
    method = request.method.lower()        
    path = os.path.join(*["api", path])
    node_descriptor = gravity_app.get_descriptor(method, path)
    
    if not node_descriptor:
        return abort(404)   

    if "debug" in request.args:
        d = copy.deepcopy(node_descriptor)
        del d["_validators"]
        return json.dumps(d)

    ctx = create_gravity_context(request, namespace, path, node_descriptor)

    execution_contexts = gravity_app.create_execution_contexts()
    rs = get_result_json(node_descriptor, execution_contexts, ctx)
    
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
        'namespace': namespace,
        'path': path         
    }

    query = {}
    for k,v in request.args.items():
        query[k] = v 

    return create_context(node_descriptor, request_body, params, query, request.headers, request.cookies)    


if __name__ == '__main__':
    app.run(debug=False)

