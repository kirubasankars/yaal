from flask import Flask
from flask import request
from flask import abort

from gravity import Gravity
from gravity import GravityConfiguration

app = Flask(__name__)
apps = {}

@app.route('/<application>/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def hello(application, path):
    try:
        g = None
        if application in apps:
            g = apps[application]            
        else:
            g = Gravity(GravityConfiguration("serve/" + application), "sqlite3")
            apps[application] = g
        
        method = request.method.lower()
        if path != "":
            path = "api/" + path
            
        e = g.create_executor(method, path, True)
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
        raise e

if __name__ == '__main__':
    app.run()
