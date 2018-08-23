import re, argparse

from gravity import Gravity, create_input_shape, get_result_json

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path')
    parser.add_argument('--method', help='method')
    args = parser.parse_args()

    args.path = "api/film"
    args.method = "get"

    if not (args.path or args.method):
        parser.print_help()
        exit() 
    
    root_path = "serve/pos"
    app = Gravity(root_path, None)
    node_descriptor = app.create_descriptor(args.method, args.path, False)
    execution_contexts = app.create_execution_contexts()                        
    input_shape = create_input_shape(node_descriptor, { "name" : "Kiruba" }, None, None, None)        
    #input_shape.validate()
    print(get_result_json(node_descriptor, execution_contexts, input_shape))