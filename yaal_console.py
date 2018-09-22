from yaal import Yaal, create_context, get_result_json

if __name__ == '__main__':
    path = "film/get"
    root_path = "serve/api"
    y = Yaal(root_path, None, False)
    descriptor = y.create_descriptor(path)
    print(descriptor)
    if not descriptor:
        print("descriptor is not found")
        exit()

    context = create_context(descriptor)
    print(get_result_json(descriptor, y.get_data_provider, context))
