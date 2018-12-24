import re
import os


def build_api_meta(y):
    root_path = y.get_root_path()
    all_paths = sorted([x[0] for x in os.walk(root_path)])
    paths = {}
    method_rx = re.compile("/(?P<method>get|put|post|delete|head|options)$")

    for p in all_paths:
        ma = method_rx.search(p)
        if ma:
            d = ma.groupdict()
            p = p.replace(root_path + "/", "")
            p = method_rx.sub("", p)
            path = "/api/" + p

            descriptor = y.create_descriptor(p + "/" + d["method"])
            if descriptor:

                if path not in paths:
                    paths[path] = {}

                if len(descriptor["model"]["payload"]["properties"]) > 0:
                    payload = descriptor["model"]["payload"]
                else:
                    payload = None

                paths[path][d["method"]] = {
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

                parameters = paths[path][d["method"]]["parameters"]

                for k, v in descriptor["model"]["query"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "query",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["header"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "header",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["cookie"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "cookie",
                        "schema": {
                            "type": v["type"]
                        }
                    })

                for k, v in descriptor["model"]["path"]["properties"].items():
                    parameters.append({
                        "name": k,
                        "in": "path",
                        "schema": {
                            "type": v["type"]
                        }
                    })

        routes = y.get_routes()
        if routes:
            for r in routes:
                p = "/api/" + r["descriptor"]
                path = "/api/" + r["path"]
                if p in paths:
                    paths[path] = paths[p]

    return paths
