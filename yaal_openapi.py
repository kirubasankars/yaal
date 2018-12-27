import copy
import os
import re


def build_api_meta(y):
    root_path = y.get_root_path()
    available_paths = sorted([x[0] for x in os.walk(root_path)])
    paths = {}
    method_rx = re.compile("/(?P<method>get|put|post|delete|head|options)$")

    routes = y.get_routes()
    routes_dict = {"/api/" + r["descriptor"]: r for r in routes}

    for p in available_paths:
        required_path_values = False
        method_match = method_rx.search(p)

        if method_match:
            method_match_dict = method_match.groupdict()
            part = p.replace(root_path + "/", "")
            part = method_rx.sub("", part)
            uri = "/api/" + part
            method = method_match_dict["method"]
            descriptor = y.create_descriptor(part + "/" + method, None)
            if descriptor:

                if uri not in paths:
                    paths[uri] = {}

                if len(descriptor["model"]["payload"]["properties"]) > 0:
                    payload = descriptor["model"]["payload"]
                else:
                    payload = None

                paths[uri][method] = {
                    "parameters": [],
                    "requestBody": {
                        "content": {
                            "application/json": payload
                        }
                    },
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": descriptor["model"]["output"]
                            }
                        }
                    }
                }

                parameters = paths[uri][method]["parameters"]

                for k, v in descriptor["model"]["query"]["properties"].items():
                    if "name" in v:
                        del v["name"]

                    parameters.append({
                        "name": k,
                        "in": "query",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["header"]["properties"].items():
                    if "name" in v:
                        del v["name"]
                    parameters.append({
                        "name": k,
                        "in": "header",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["cookie"]["properties"].items():
                    if "name" in v:
                        del v["name"]
                    parameters.append({
                        "name": k,
                        "in": "cookie",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["path"]["properties"].items():
                    if "name" in v:
                        del v["name"]

                    if "required" in v:
                        required_path_values = True

                    parameters.append({
                        "name": k,
                        "in": "path",
                        "schema": {
                            "type": v["type"]
                        }
                    })


            if uri in routes_dict:
                route = routes_dict[uri]
                route_path = "/api/" + route["path"]

                if route_path not in paths:
                    paths[route_path] = {}

                if required_path_values:
                    if method not in paths[route_path]:
                        paths[route_path][method] = copy.deepcopy(paths[uri][method])
                        del paths[uri][method]
                # else:
                #    if method not in paths[route_path]:
                #        paths[route_path][method] = copy.deepcopy(paths[uri][method])

    return paths
