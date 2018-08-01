from flask import Flask
from flask import request
from flask import abort

from gravity import Gravity
from gravity import GravityConfiguration
from gravity import SQLiteExecutionContext

app = Flask(__name__)
g = Gravity(GravityConfiguration("serve"))
execution_context = SQLiteExecutionContext()

@app.route('/<app_name>/api/', defaults={'app_name':'','path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<app_name>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def hello(app_name, path):
    try:
        descriptor = g.create_descriptor(request.method.lower(), app_name + "/api/" + path)
        if descriptor is None:
            return ""

        e = descriptor.create_executor(execution_context)

        input_shape = None
        try:
            ijson = request.get_json()                    
            input_shape = e.create_input_shape(ijson)
        except:
            input_shape = e.create_input_shape(None)

        for k, v in request.args.items():
            input_shape.set_prop(k, v)
    
        return e.get_result_json(input_shape)        
    except Exception as e:
        print(e)

if __name__ == '__main__':
    app.run()
