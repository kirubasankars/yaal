import argparse

from gravity import Gravity, GravityConfiguration
from executioncontext import SQLiteExecutionContext
from contentreader import FileReader

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path')
    parser.add_argument('--method', help='method')
    args = parser.parse_args()

    args.path = "api/customer"
    args.method = "get"

    if args.path is None or args.method is None:
        parser.print_help()
        #exit() 
    
    gravity_configuration = GravityConfiguration("serve/pos")
    gravity = Gravity(gravity_configuration, FileReader(gravity_configuration))
    executor = gravity.create_executor(args.method, args.path, False)

    execution_contexts = gravity.create_execution_contexts()

    if executor is not None:                
        input_shape = executor.create_input_shape({"page":0})
        executor.get_result_json(execution_contexts, input_shape)