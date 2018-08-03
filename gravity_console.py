import argparse

from gravity import Gravity, GravityConfiguration
from executioncontext import SQLiteExecutionContext

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path')
    parser.add_argument('--method', help='method')
    args = parser.parse_args()
    
    if args.path is None or args.method is None:
        parser.print_help()
        exit() 
    
    #args.path = "app/api/get1"
    #args.method = "get"

    gravity = Gravity(GravityConfiguration("serve"))
    execution_context = SQLiteExecutionContext()
    descriptor = gravity.create_descriptor(args.method, args.path)

    if descriptor is not None:        
        executor = descriptor.create_executor(execution_context)
        input_shape = executor.create_input_shape(None)
        print(executor.get_result_json(input_shape))