import json

import yaal_const


def _to_lower_keys(obj):
    if obj:
        if type(obj) == dict:
            return {k.lower(): v for k, v in obj.items()}
        elif type(obj) == list and type(obj[0]) == dict:
            return [_to_lower_keys(item) for item in obj]
    return obj


def _to_lower_keys_deep(obj):
    if obj:
        if type(obj) == dict:
            o = {}
            for k, v in obj.items():
                if type(v) == list or type(v) == dict:
                    o[k.lower()] = _to_lower_keys(v)
                else:
                    o[k.lower()] = v
            return o
        elif type(obj) == list and type(obj[0]) == dict:
            return [_to_lower_keys(item) for item in obj]
    return obj


class Shape:

    def __init__(self, schema=None, data=None, validator=None, parent_shape=None, extras=None):
        self._array = False
        self._object = False
        self._input_properties = None
        self._index = 0

        self._schema = schema
        self._validator = validator

        self._parent = parent_shape
        self._data = data or {}
        self._o_data = data or {}

        self._extras = extras

        # TODO: CRITICAL implement no properties start with $ is allowed
        if data is not None \
                and (yaal_const.PARENT in data or yaal_const.LENGTH in data or
                     yaal_const.JSON in data or yaal_const.INDEX in data):
            raise ValueError("$parent or $length is reversed keywords. You can't use them.")

        if extras and len([e for e in extras if type(extras[e]) != Shape]):
            raise TypeError("$extra should be type shape.")

        if parent_shape is not None and type(parent_shape) != Shape:
            raise TypeError("$parent should be type shape.")

        schema = schema or {"type":"object"}

        if yaal_const.PROPERTIES in schema:
            self._input_properties = schema[yaal_const.PROPERTIES]
        self._input_properties = self._input_properties or {}

        _type = schema[yaal_const.TYPE]
        if _type == yaal_const.ARRAY:
            self._array = True
            if data:
                if type(data) != list:
                    raise TypeError("input expected as array. object is given.")
        else:
            self._object = True
            if data:
                if type(data) == dict:
                    self._data = _to_lower_keys(data)
                else:
                    raise TypeError("input expected as object. " + str(type(data)) + " is given.")

        if self._array:
            shapes = []
            schema[yaal_const.TYPE] = yaal_const.OBJECT
            idx = 0
            for item in self._data:
                s = Shape(schema=schema, data=item, parent_shape=self, extras=extras)
                s._index = idx
                idx = idx + 1
                shapes.append(s)
            schema[yaal_const.TYPE] = yaal_const.ARRAY
        else:
            shapes = {}
            for k, v in self._input_properties.items():
                k = k.lower()
                if type(v) == dict:
                    _type_value = v.get(yaal_const.TYPE)
                    if _type_value and _type_value == yaal_const.ARRAY or _type_value == yaal_const.OBJECT:
                        shapes[k] = Shape(schema=v, data=self._data.get(k), parent_shape=self, extras=extras)

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

            if self._array:
                try:
                    idx = int(path[1:])
                except:
                    raise KeyError("array path excepted as $index.")
                return shapes[idx].get_prop(remaining_path)

            if path[0] == "$":
                if path == yaal_const.PARENT:
                    return parent.get_prop(remaining_path)

                if extras:
                    if path in extras:
                        return extras[path].get_prop(remaining_path)

            return shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":

                if prop == yaal_const.JSON or prop == yaal_const.PARENT \
                        or prop == yaal_const.LENGTH or prop == yaal_const.INDEX:
                    if prop == yaal_const.JSON:
                        return json.dumps(self.get_data())
                    if prop == yaal_const.PARENT:
                        return parent
                    if prop == yaal_const.LENGTH:
                        return len(data)
                    if prop == yaal_const.INDEX:
                        return self._index

                if extras:
                    if prop in extras:
                        return extras[prop]

            if self._array:
                try:
                    idx = int(prop[1:])
                except:
                    raise KeyError("array path excepted as $index.")
                return shapes[idx]

            if prop in shapes:
                return shapes[prop]

            if prop in data:
                return data[prop]

            if prop in self._input_properties:
                default_str = "default"
                property_schema = self._input_properties[prop]
                if default_str in property_schema:
                    return property_schema[default_str]

            return None

    def set_prop(self, prop, value):
        shapes = self._shapes

        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot + 1:]

            if self._array:
                try:
                    idx = int(path[1:])
                except:
                    raise KeyError("array path excepted as $index.")
                return shapes[idx].set_prop(remaining_path, value)

            if path in shapes:
                return shapes[path].set_prop(remaining_path, value)
            else:
                if path in self._extras:
                    self._extras[path].set_prop(remaining_path, value)
        else:
            value = self._type_cast(prop, value)
            self._data[prop.lower()] = value

            if prop.lower() in self._o_data:
                del self._o_data[prop.lower()]
            self._o_data[prop] = value

    def validate(self, include_extras=False):
        errors = []

        extras = self._extras
        if extras and include_extras:
            for name, extra in extras.items():
                for x in extra.validate(include_extras):
                    x["name"] = name
                    errors.append(x)

        if self._validator:
            error_list = list(self._validator.iter_errors(self._data))
            if error_list:
                for x in error_list:
                    m = {
                        "message": x.message
                    }
                    errors.append(m)

        return errors

    def get_data(self):
        return self._o_data

    def _type_cast(self, prop, value):
        if prop in self._input_properties:
            prop_schema = self._input_properties[prop]
            parameter_type = prop_schema.get(yaal_const.FORMAT)
            if not parameter_type:
                parameter_type = prop_schema.get(yaal_const.TYPE)
            try:
                if value is not None:
                    if parameter_type == "integer" and not isinstance(value, int):
                        return int(value)
                    if parameter_type == "string" and not isinstance(value, str):
                        return str(value)
                    if parameter_type == "float" and not isinstance(value, float):
                        return float(value)
                    if parameter_type == "boolean" and not isinstance(value, bool):
                        return bool(value)
            except ValueError:
                raise ValueError("value expected as " + parameter_type + ", given " + str(type(value)))
        return value

    def get_schema(self):
        return self._schema

    def get_validator(self):
        return self._validator

    def __str__(self):
        return json.dumps(self.get_data())
