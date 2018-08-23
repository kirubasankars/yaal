import re
import argparse

from gravity import Gravity
from executioncontext import SQLiteExecutionContext
from contentreader import FileReader

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path')
    parser.add_argument('--method', help='method')
    args = parser.parse_args()

    args.path = "api/"
    args.method = "get"

    if not (args.path or args.method):
        parser.print_help()
        #exit() 
    
    root_path = "serve/pos"
    gravity = Gravity(root_path, FileReader(root_path))
    executor = gravity.create_executor(args.method, args.path, False)

    execution_contexts = gravity.create_execution_contexts()

    if executor:                
        input_shape = executor.create_input_shape({ "name" : "Kiruba" }, None, None, None)        
        input_shape.validate()
        print(executor.get_result_json(execution_contexts, input_shape))