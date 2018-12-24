import json


def _to_lower_keys(obj):
    if obj:
        if type(obj) == dict:
            return {k.lower(): v for k, v in obj.items()}
        elif type(obj) == list and type(obj[0]) == dict:
            return [_to_lower_keys(item) for item in obj]
    return obj


class Shape:

    def __init__(self, schema, data, parent_shape, extras, validator):
        self._array = False
        self._object = False

        self._data = data or {}
        self._o_data = data or {}
        self._parent = parent_shape
        self._schema = schema
        self._input_properties = None
        input_properties = None
        self._index = 0
        self._validator = validator

        self._extras = extras

        if data is not None and ("$parent" in data or "$length" in data):
            raise Exception("$parent or $length is reversed keywords. You can't use them.")

        schema = schema or {}

        _properties_str = "properties"
        if _properties_str in schema:
            input_properties = schema[_properties_str]
            self._input_properties = input_properties

        _type_str = "type"
        if _type_str in schema:
            _type = schema[_type_str]
            if _type == "array":
                self._array = True
                if data and type(data) != list:
                    raise TypeError("input expected as array. object is given.")
            else:
                self._object = True
                if data:
                    if type(data) != dict:
                        raise TypeError("input expected as object. array is given.")
                    else:
                        self._data = _to_lower_keys(data)

        if self._array:
            shapes = []
            schema[_type_str] = "object"
            idx = 0
            for item in self._data:
                s = Shape(schema, item, self, extras, None)
                s._index = idx
                shapes.append(s)
                idx = idx + 1
            schema[_type_str] = "array"
        else:
            shapes = {}
            if input_properties:
                for k, v in input_properties.items():
                    if type(v) == dict:
                        _type_value = v.get(_type_str)
                        if _type_value and _type_value == "array" or _type_value == "object":
                            shapes[k.lower()] = Shape(v, self._data.get(k.lower()), self, extras, None)

        self._shapes = shapes

    def get_prop(self, prop):
        extras = self._extras
        shapes = self._shapes
        data = self._data
        parent = self._parent

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot + 1:]

            if path[0] == "$":
                if path == "$parent":
                    return parent.get_prop(remaining_path)

                if extras:
                    if path in extras:
                        return extras[path].get_prop(remaining_path)

            if self._array:
                idx = int(path[1:])
                return shapes[idx].get_prop(remaining_path)

            return shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":

                if prop == "$json" or prop == "$parent" or prop == "$length" or prop == "$index":
                    if prop == "$json":
                        return json.dumps(self.get_data())
                    if prop == "$parent":
                        return parent
                    if prop == "$length":
                        return len(data)
                    if prop == "$index":
                        return self._index

                if extras:
                    if prop in extras:
                        return extras[prop]

            if self._array:
                idx = int(prop[1:])
                return shapes[idx]

            if prop in shapes:
                return shapes[prop]

            if prop in data:
                return data[prop]

            if self._input_properties is not None and prop in self._input_properties:
                default_str = "default"
                input_type = self._input_properties[prop]
                if default_str in input_type:
                    return input_type[default_str]

            return None

    def set_prop(self, prop, value):
        shapes = self._shapes

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot + 1:]

            if path in shapes:
                if self._array:
                    idx = int(path[1:])
                    return shapes[idx].set_prop(remaining_path, value)
                else:
                    return shapes[path].set_prop(remaining_path, value)
            else:
                if path in self._extras:
                    self._extras[path].set_prop(remaining_path, value)
        else:
            v = self.check_and_cast(prop, value)
            self._data[prop.lower()] = v
            self._o_data[prop] = v

    def validate(self, include_extras):
        errors = []

        extras = self._extras
        if extras and include_extras:
            for name, extra in extras.items():
                for x in extra.validate(include_extras):
                    errors.append(x)

        if self._validator:
            error_list = list(self._validator.iter_errors(self._data))
            if error_list:
                for x in error_list:
                    p = None
                    if len(x.path) > 0:
                        p = x.path[0]

                    m = {
                        "path": p,
                        "message": x.message
                    }
                    errors.append(m)

        return errors

    def get_data(self):
        return self._o_data

    def check_and_cast(self, prop, value):
        if self._input_properties is not None and prop in self._input_properties:
            prop_schema = self._input_properties[prop]
            _type_str = "type"
            if _type_str in prop_schema:
                parameter_type = prop_schema[_type_str]
                try:
                    if parameter_type == "integer" and not isinstance(value, int):
                        return int(value)
                    if parameter_type == "string" and not isinstance(value, str):
                        return str(value)
                    if parameter_type == "number" and not isinstance(value, float):
                        return float(value)
                except ValueError:
                    pass
        return value

    def __str__(self):
        return json.dumps(self.get_data())

