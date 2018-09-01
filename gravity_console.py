import re, argparse, json

from gravity import Gravity, create_context, get_result_json

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
    app = Gravity(root_path, None, False)
    descriptor = app.create_descriptor(args.method, args.path)
    data_providers = app.get_data_providers()
    context = create_context(descriptor, { "page" : 1 }, None, { "page" : 1 }, None, None)    
    print(get_result_json(descriptor, data_providers, context))
    #print(json.dumps(node_descriptor))