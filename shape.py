

class Shape:
    def __init__(self, input_model, data, parent_shape):        
        self._list = False
        self._object = False

        self.data = data or {}
        self.shapes = {}
        self._parent = parent_shape
        self._input_model = input_model
        
        if input_model is None:
            return
        
        _propertiesstr = "properties"
        if _propertiesstr in input_model:
            self.input_properties = input_model[_propertiesstr]

        _typestr = "type"
        if _typestr in input_model:
            _type = input_model[_typestr]
            if _type == "array":
                self._list = True                
            else:
                self._object = True
        
        if self._list and data is not None:            
            idx = 0
            input_model[_typestr] = "object"
            if type(data) != list:
                raise TypeError("input expected as array. object is given.")
            for item in data:
                self.shapes["@" + str(idx)] = Shape(input_model, item, self)                
                idx = idx + 1
            input_model[_typestr] = "array"
        else:
            input_properties = self.input_properties
            if input_properties is not None:
                for k, v in input_properties.items():
                    if type(v) == dict and _propertiesstr in v:                            
                        dvalue = None
                        if data is not None and k in data:
                            dvalue = data.get(k)
                        self.shapes[k] = Shape(v, dvalue, self)

    def get_prop(self, prop):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path == "$parent":
                return self._parent.get_prop(remaining_path)

            if path in self.shapes:
                return self.shapes[path].get_prop(remaining_path)
        else:
            if prop == "$parent":
                return self._parent

            if prop == "$length":
                return len(self.data)

            if prop in self.shapes:
                return self.shapes[prop]

            if prop in self.data:
                return self.data[prop]
                        
            if prop in self.input_properties:
                prop_type = self.input_properties[prop]
                _defaultstr = "default"
                if _defaultstr in prop_type:
                    return prop_type[_defaultstr]

            return None

    def set_prop(self, prop, value):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path == "$parent":
                return self._parent.set_prop(remaining_path, value)

            if path in self.shapes:
                return self.shapes[path].set_prop(remaining_path, value)
        else:                       
            self.data[prop] = value
