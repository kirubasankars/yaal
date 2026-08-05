import copy
import datetime
import json

from collections import defaultdict

from yaal_parser import compile_sql


class DataProviderHelper:

    def __init__(self):
        self._cache = {}

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def get_executable_content(char, twig, input_shape):
        nulls = []
        if "nullable" in twig:
            for n in twig["nullable"]:
                if input_shape.get_prop(n) is None:
                    nulls.append(n)
        return compile_sql(twig, nulls, char)

    def build_parameters(self, query, input_shape, get_value_converter):
        values = []
        _cache = self._cache
        if "parameters" in query:
            parameters = query["parameters"]
            for p in parameters:
                param_name = p["name"]
                param_type = p["type"]

                if param_name in _cache:
                    param_value = _cache[param_name]
                else:
                    param_value = input_shape.get_prop(param_name)
                    # Cache only request-scoped $-params, never payload fields or $parent paths.
                    if param_name.startswith("$") and "$parent" not in param_name:
                        _cache[param_name] = param_value

                try:
                    if param_value is not None:
                        if param_type == "integer":
                            param_value = int(param_value)
                        elif param_type == "string":
                            param_value = str(param_value)
                        else:
                            param_value = get_value_converter(param_type, param_value)
                    values.append(param_value)
                except ValueError:
                    values.append(param_value)

        return values


def _execute_twigs(branch, data_providers, context, data_provider_helper):
    errors = []

    twigs = branch.get("twigs")
    action_str = "$action"
    params_str, error_str, break_str = "params", "error", "break"
    json_str = "json"

    rs = []
    if twigs:
        for twig in twigs:

            connection = twig["connection"]
            output, output_last_inserted_id = data_providers[connection].execute(twig, context, data_provider_helper)

            context.get_prop("$params").set_prop("$last_inserted_id", output_last_inserted_id)

            if len(output) >= 1:
                output0 = output[0]
                if action_str in output0:
                    action_value = output0[action_str]
                    if action_value == error_str:
                        errors.extend(output)
                        return None, errors
                    elif action_value == json_str:
                        json_list = []
                        if type(output0[json_str]) == str:
                            for o in output:
                                json_list.append(json.loads(o[json_str]))
                        else:
                            json_list.extend([o[json_str] for o in output])
                        return json_list, None

                    elif action_value == break_str:
                        for o in output:
                            del o[action_str]
                        return output, None
                    elif action_value == params_str:
                        params = context.get_prop("$params")
                        for k, v in output0.items():
                            params.set_prop(k, v)
                else:
                    rs = output

    return rs, None


def _trunk_cleanup(data_providers, db_data_provider, failed):
    if failed:
        try:
            db_data_provider.error()
        except Exception:
            pass
        for name, data_provider in data_providers.items():
            if name != "db":
                try:
                    data_provider.error()
                except Exception:
                    pass
        return

    db_data_provider.end()
    for name, data_provider in data_providers.items():
        if name != "db":
            data_provider.end()


def _execute_branch(branch, is_trunk, data_providers, context, parent_rows, cache_provider):
    input_type, output_partition_by, cache = branch["input_type"], branch.get("partition_by"), branch.get("cache")
    use_parent_rows, method = branch.get("use_parent_rows"), branch.get("method")
    output = []
    data_provider_helper = DataProviderHelper()
    db_data_provider = data_providers["db"]
    began = False
    failed = False

    try:
        if cache and method in cache_provider:
            output = copy.deepcopy(cache_provider[method])
        elif use_parent_rows:
            output = copy.deepcopy(parent_rows)
        else:
            if is_trunk:
                for name, data_provider in data_providers.items():
                    data_provider.begin()
                began = True

            if input_type == "array":
                length = int(context.get_prop("$length"))
                for i in range(0, length):
                    data_provider_helper.clear_cache()
                    item_ctx = context.get_prop("@" + str(i))
                    rs, errors = _execute_twigs(branch, data_providers, item_ctx, data_provider_helper)
                    if errors:
                        failed = True
                        return None, errors
                    output.extend(rs)

            elif input_type == "object":
                output, errors = _execute_twigs(branch, data_providers, context, data_provider_helper)
                if errors:
                    failed = True
                    return None, errors

            if cache:
                cache_provider[method] = copy.deepcopy(output)

        branches = branch.get("branches")
        if branches:
            for branch_descriptor in branches:
                branch_name = branch_descriptor["name"]
                sub_node_shape = context
                if context:
                    nested = context.get_prop(branch_name.lower())
                    if nested is not None:
                        sub_node_shape = nested

                sub_node_output, errors = _execute_branch(
                    branch_descriptor, False, data_providers, sub_node_shape, output, cache_provider
                )
                if errors:
                    failed = True
                    return None, errors

                if not branch.get("twigs") and not use_parent_rows and not output:
                    output.append({})

                if not output_partition_by:
                    for row in output:
                        row[branch_name] = copy.deepcopy(sub_node_output)
                else:
                    sub_node_groups = defaultdict(list)
                    for row in sub_node_output:
                        if output_partition_by not in row:
                            raise KeyError(
                                "partition_by column '%s' missing from child row" % output_partition_by
                            )
                        sub_node_groups[row[output_partition_by]].append(row)

                    groups = defaultdict(list)
                    for row in output:
                        if output_partition_by not in row:
                            raise KeyError(
                                "partition_by column '%s' missing from parent row" % output_partition_by
                            )
                        groups[row[output_partition_by]].append(row)

                    _output = []
                    for idx, rows in groups.items():
                        row = rows[0]
                        partition_key = row[output_partition_by]
                        row[branch_name] = copy.deepcopy(sub_node_groups.get(partition_key, []))
                        _output.append(row)
                    output = _output

        return output, None

    except Exception:
        failed = True
        raise
    finally:
        if is_trunk and began:
            _trunk_cleanup(data_providers, db_data_provider, failed)


def _output_mapper(output_type, output_modal, branches, result):
    mapped_result = []

    _type_str, _mapped_str = "type", "mapped"

    output_model = output_modal
    if output_model and "properties" in output_model:
        output_properties = output_model["properties"]
    else:
        output_properties = None

    output_type = output_type

    for row in result:
        mapped_obj = {}
        mapped_tree = {}
        if branches:
            for branch_descriptor in branches:

                branch_name = branch_descriptor["name"]
                branch_output_type = branch_descriptor["output_type"]

                if output_properties and branch_name in output_properties:
                    branch_output_model = output_properties[branch_name]
                else:
                    branch_output_model = None

                branch_descriptor_branches = branch_descriptor.get("branches")

                if branch_name in row:
                    mapped_tree[branch_name] = _output_mapper(
                        branch_output_type,
                        branch_output_model,
                        branch_descriptor_branches,
                        row[branch_name],
                    )

        if output_properties:
            prop_count = 0
            for k, v in output_properties.items():

                _mapped, _type = None, None

                if type(v) == str:
                    _mapped = v
                if type(v) == dict:
                    _mapped = v.get(_mapped_str)
                    _type = v.get(_type_str)

                if _mapped:
                    if _mapped in row:
                        mapped_obj[k] = row[_mapped]
                        prop_count = prop_count + 1
                    else:
                        raise Exception(_mapped + " _mapped column missing from row")

                if _type and (_type == "array" or _type == "object"):
                    mapped_obj[k] = mapped_tree[k]

            if prop_count == 0:
                mapped_obj = row
        else:
            mapped_obj = row

        for k, v in mapped_tree.items():
            mapped_obj[k] = v

        mapped_result.append(mapped_obj)

    if output_type == "object":
        if len(mapped_result) > 0:
            mapped_result = mapped_result[0]
        else:
            mapped_result = {}
    return mapped_result


def _get_result(descriptor, get_data_provider, ctx, cache_provider):
    errors = []
    args_shape = ctx.get_prop("$args")
    if args_shape is not None:
        errors.extend(args_shape.validate(True))
    errors.extend(ctx.validate(False))

    if errors:
        return {"errors": errors}

    data_providers = {}
    for con in descriptor["connections"]:
        data_providers[con] = get_data_provider(con)

    rs, errors = _execute_branch(descriptor, True, data_providers, ctx, [], cache_provider)

    if errors:
        return {"errors": errors}

    rs = _output_mapper(descriptor["output_type"], descriptor["model"]["output"], descriptor.get("branches"), rs)

    return rs


def _default_date_time_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def get_result(descriptor, get_data_provider, context, cache_provider=None):
    return _get_result(descriptor, get_data_provider, context, cache_provider or {})


def get_result_json(descriptor, get_data_providers, context, cache_provider=None):
    return json.dumps(get_result(descriptor, get_data_providers, context, cache_provider),
                      default=_default_date_time_converter)
