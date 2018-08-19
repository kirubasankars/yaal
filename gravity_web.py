import os 
from flask import Flask, request, abort, send_from_directory

from gravity import Gravity
from gravity import GravityConfiguration
from contentreader import FileReader
from shape import Shape

app = Flask(__name__)
apps = {}

@app.route('/<namespace>', methods=['GET'])
def root(namespace):
    static_file_dir = os.path.join("serve", namespace, 'app')
    return send_from_directory(static_file_dir, 'index.html')
 
@app.route('/<namespace>/<path:path>', methods=['GET'])
def serve_app(namespace, path):
    static_file_dir = os.path.join("serve", namespace, 'app')
    if not os.path.isfile(os.path.join(static_file_dir, path)):
        path = os.path.join(path, 'index.html')
 
    return send_from_directory(static_file_dir, path)

@app.route('/<namespace>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def serve_api(namespace, path):
    try:
        g = None
        if namespace in apps:
            g = apps[namespace]            
        else:
            gc = GravityConfiguration("serve/" + namespace)
            g = Gravity(gc, FileReader(gc))
            apps[namespace] = g
        
        method = request.method.lower()
        if path != "":
            path = "api/" + path
            
        e = g.create_executor(method, path, True)
        input_shape = None
        request_body = None
        try:
            request_body = request.get_json()                                
        except:
            pass

        query = {}
        for k, v in request.args.items():
            query[k] = v

        params = {
            "namespace": namespace,
            "path": path            
        }

        input_shape = e.create_input_shape(request_body, params, query, query)
        execution_contexts = g.create_execution_contexts()
        return e.get_result_json(execution_contexts, input_shape)
    except Exception as e:        
        raise e

if __name__ == '__main__':
    app.run(debug=False)
