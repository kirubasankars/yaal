

class Shape:
    def __init__(self, input_model, data, parent_shape):        
        self._list = False
        self._object = False

        self._data = data or {}    
        self._shapes = {}
        shapes = self._shapes
        self._parent = parent_shape
        self._input_model = input_model        
        self._input_properties = None
        input_properties = None
        
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
                self._shapes["@" + str(idx)] = Shape(input_model, item, self)                
                idx = idx + 1
            input_model[_typestr] = "array"
        else:            
            if input_properties is not None:
                for k, v in input_properties.items():
                    if type(v) == dict and _propertiesstr in v:                            
                        dvalue = None
                        if k in self._data:
                            dvalue = data.get(k)
                        shapes[k] = Shape(v, dvalue, self)            

    def get_prop(self, prop):
        dot = prop.find(".")
        if dot > -1:
            path = prop[:dot]
            remaining_path = prop[dot+1:]
            
            if path == "$parent":
                return self._parent.get_prop(remaining_path)

            if path in self._shapes:
                return self._shapes[path].get_prop(remaining_path)
        else:

            if prop[0] == "$":
                if prop == "$parent":
                    return self._parent

                if prop == "$length":
                    return len(self._data)

            if prop in self._shapes:
                return self._shapes[prop]

            if prop in self._data:
                return self._data[prop]

            if prop in self._input_properties:
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
            
            if path == "$parent":
                return self._parent.set_prop(remaining_path, value)

            if path in self._shapes:
                return self._shapes[path].set_prop(remaining_path, value)
        else:                                   
            self._data[prop] = value
