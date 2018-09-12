
from gravity import Gravity, create_context, get_result_json

if __name__ == '__main__':

    path = "film/get"
    root_path = "serve/pos"
    app = Gravity(root_path, None, False)
    descriptor = app.create_descriptor(path)
    context = create_context(descriptor, {"page": 1}, None, None, None, None, None, None)
    print(get_result_json(descriptor, app.get_data_provider, context))