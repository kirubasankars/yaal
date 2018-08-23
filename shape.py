from jsonschema import validate, FormatChecker

class Shape:
    
    def __init__(self, input_model, data, parent_shape, params_shape, query_shape, path_shape):        
        self._list = False
        self._object = False

        self._data = data or {}
        self._shapes = {}
        shapes = self._shapes
        self._parent = parent_shape        
        self._input_model = input_model        
        self._input_properties = None
        input_properties = None
        
        self._params = params_shape
        self._query = query_shape
        self._path = path_shape
        
        if data is not None and ("$parent" in data or "$length" in data):
            raise Exception("$parent or $length is reversed keywords. You can't use them.")

        if input_model is None:
            return
        
        _propertiesstr = "properties"
        if _propertiesstr in input_model:
            input_properties = input_model[_propertiesstr]
            self._input_properties = input_properties


        _typestr = "type"
        if _typestr in input_model:
            _type = input_model[_typestr]
            if _type == "array":
                self._list = True                
                if data is not None:
                    if type(data) != list:
                        raise TypeError("input expected as array. object is given.")
            else:
                self._object = True
                if data is not None:
                    if type(data) != dict:
                        raise TypeError("input expected as object. array is given.")
        
        if self._list:                
            idx = 0
            input_model[_typestr] = "object"                
            for item in self._data:
                self._shapes["@" + str(idx)] = Shape(input_model, item, self, self._params, self._query, self._path)
                idx = idx + 1
            input_model[_typestr] = "array"
        else:            
            if input_properties is not None:
                for k, v in input_properties.items():
                    if type(v) == dict and _propertiesstr in v:                            
                        dvalue = None
                        if k in self._data:
                            dvalue = data.get(k)
                        shapes[k] = Shape(v, dvalue, self, self._params, self._query, self._path)            

    def set_query(self, query_shape):
        self._query = query_shape

    def set_path(self, path_shape):
        self._path = path_shape

    def get_prop(self, prop):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path[0] == "$":
                if path == "$parent":
                    return self._parent.get_prop(remaining_path)

                if path == "$params":
                    return self._params.get_prop(remaining_path)

                if path == "$query":
                    return self._query.get_prop(remaining_path)

                if path == "$path":
                    return self._path.get_prop(remaining_path)

            if path in self._shapes:
                return self._shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":
                if prop == "$parent":
                    return self._parent

                if prop == "$params":
                    return self._params

                if prop == "$query":
                    return self._query

                if prop == "$path":
                    return self._path

                if prop == "$length":
                    return len(self._data)
                                

            if prop in self._shapes:
                return self._shapes[prop]

            if prop in self._data:
                return self._data[prop]

            if self._input_properties is not None and prop in self._input_properties:
                defaultstr = "default"
                input_type = self._input_properties[prop]
                if defaultstr in input_type:
                    return input_type[defaultstr]
            
            return None

    def set_prop(self, prop, value):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path[0] == "$":
                if path == "$params":
                    return self._params.set_prop(remaining_path, value)

            if path in self._shapes:
                return self._shapes[path].set_prop(remaining_path, value)
        else:            
            self._data[prop] = self.check_and_cast(prop, value)

    def validate(self):
        errors = { }
        
        if self._query:
            errors["query"] = self._query.validate()
        if self._path:
            errors["path"] = self._path.validate()
        if self._params:
            errors["params"] = self._params.validate()

        if self._input_model is not None:          
            validate(self._data, self._input_model, format_checker=FormatChecker())

    def check_and_cast(self, prop, value):
        if self._input_properties is not None and prop in self._input_properties:
                prop_schema = self._input_properties[prop]
                _type_str = "type"
                if _type_str in prop_schema:
                    ptype = prop_schema[_type_str]
                    try:
                        if ptype  == "integer" and not isinstance(value, int):
                            return int(value)
                        if ptype  == "string" and not isinstance(value, str):
                            return str(value)
                        if ptype  == "number" and not isinstance(value, float):
                            return float(value)
                    except:
                        pass                    
        return value