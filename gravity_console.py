import argparse

from gravity import Gravity, GravityConfiguration

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
    
    gravity = Gravity(GravityConfiguration("serve/pos"), "sqlite3")
    executor = gravity.create_executor(args.method, args.path, False)

    if executor is not None:                
        input_shape = executor.create_input_shape({"page":6})
        print(executor.get_result_json(input_shape))