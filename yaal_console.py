from yaal import Yaal, create_context, get_result_json

if __name__ == '__main__':
    path = "film/get"
    root_path = "serve/auth"
    y = Yaal(root_path, None, False)
    descriptor = y.create_descriptor(path)
    context = create_context(descriptor, {"page": 1}, None, None, None, None, None, None)
    print(get_result_json(descriptor, y.get_data_provider, context))
