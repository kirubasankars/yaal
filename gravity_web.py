import os 
import json

from flask import Flask, request, abort, send_from_directory
from gravity import Gravity
from contentreader import FileReader
from shape import Shape

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

@app.route('/<namespace>/api', methods=['GET'], defaults = { 'path' : '' })
@app.route('/<namespace>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def serve_api(namespace, path):
    join = os.path.join

    if namespace in namespaces:
        g = namespaces[namespace]            
    else:
        root_path = join(*["serve", namespace])
        g = Gravity(root_path, FileReader(root_path))
        namespaces[namespace] = g
    
    method = request.method.lower()        
    path = join(*["api", path])
    
    executor = g.create_executor(method, path, True)
    if not executor:
        return abort(404)
    
    if request.mimetype == "application/json":
        try:
            request_body = request.get_json()
        except:
            request_body = None
    else:
        request_body = None

    query = {}
    for k, v in request.args.items():
        query[k] = v

    if request.mimetype == "multipart/form-data":
        request_body = request_body or {}
        for k, v in request.form.items():
            request_body[k] = v

    params = {
        'namespace': namespace,
        'path': path            
    }

    input_shape = executor.create_input_shape(request_body, params, query, query)
    #input_shape.validate()            
    execution_contexts = g.create_execution_contexts()
    return executor.get_result_json(execution_contexts, input_shape)

if __name__ == '__main__':
    app.run(debug=False)
