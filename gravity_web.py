from flask import Flask
from flask import request
from flask import jsonify

from gravity import Gravity
from gravity import GravityConfiguration
from gravity import SQLiteExecutionContext

app = Flask(__name__)
g = Gravity(GravityConfiguration("/home/kirubasankars/workspace/gravity/serve"))
execution_context = SQLiteExecutionContext()

@app.route('/<app_name>/api/', defaults={'app_name':'','path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<app_name>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def hello(app_name, path):    
    descriptor = g.create_descriptor(request.method.lower(), app_name + "/api/" + path)
    if descriptor is not None:        
        e = descriptor.create_executor(execution_context)

        input_shape = None
        try:
            ijson = request.get_json()                    
            input_shape = e.create_input_shape(ijson)
        except:
            input_shape = e.create_input_shape(None)

        for k, v in request.args.items():
            input_shape.set_prop(k, v)
    
        return jsonify(e.get_result(input_shape))

if __name__ == '__main__':
    app.run()
