from yaal import Yaal, create_context

YAAL_DEBUG = False

DATA_PROVIDERS = {
    "db": "sqlite3://./serve/db/app.db",
    "sqlite3": "sqlite3:///"
}

CORS_ENABLED = False

if __name__ == '__main__':
    path = "user/1"
    method = "get"
    root_path = "serve/api"

    y = Yaal(root_path, None, True)
    for name, db_url in DATA_PROVIDERS.items():
        y.setup_data_provider(name, db_url)

    descriptor_ctx = y.get_descriptor_path_by_route(path, method)
    descriptor = y.get_descriptor(descriptor_ctx)

    if not descriptor:
        print("descriptor is not found")
        exit()

    context = create_context(descriptor, path_values=descriptor_ctx["path_values"])
    print(y.get_result_json(descriptor, descriptor_ctx, context))
