import os, json 

from flask import Flask, request, abort, send_from_directory
from gravity import Gravity, create_input_shape, get_result_json

app = Flask(__name__)
namespaces = {}

@app.route('/<namespace>', methods=['GET'])
def root(namespace):
    static_file_dir = os.path.join('serve', namespace, 'app')
    return send_from_directory(static_file_dir, 'index.html')
 
@app.route('/<namespace>/<path:path>', methods=['GET'])
def serve_app(namespace, path):
    static_file_dir = os.path.join('serve', namespace, 'app')
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, 'index.html')
 
    return send_from_directory(static_file_dir, path)

def remove_nulls(d):
    return {k: v for k, v in d.iteritems() if v is not None}

@app.route('/<namespace>/api/', methods=['GET'], defaults = { 'path' : '' })
@app.route('/<namespace>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def serve_api(namespace, path):
    join = os.path.join

    if namespace in namespaces:
        app = namespaces[namespace]            
    else:
        root_path = join(*["serve", namespace])
        app = Gravity(root_path, None)
        namespaces[namespace] = app
    
    method = request.method.lower()        
    path = join(*["api", path])
    
    node_descriptor = app.create_descriptor(method, path, False)
    if not node_descriptor:
        return abort(404)
    
    args = request.args
    if "debug" in args:
        return json.dumps(node_descriptor)

    if request.mimetype == "application/json":
        try:
            request_body = request.get_json()
        except:
            request_body = None
    else:
        request_body = None

    query = {}
    for k, v in args.items():
        query[k] = v

    if request.mimetype == "multipart/form-data":
        request_body = request_body or {}
        for k, v in request.form.items():
            request_body[k] = v

    params = {
        'namespace': namespace,
        'path': path            
    }

    input_shape = create_input_shape(node_descriptor, request_body, params, query, query)    
    execution_contexts = app.create_execution_contexts()
    return get_result_json(node_descriptor, execution_contexts, input_shape)

if __name__ == '__main__':
    app.run(debug=False)
