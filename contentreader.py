import os, yaml, json

class FileReader:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_sql(self, method, path):
        file_path = os.path.join(*[self._root_path, path, method + ".sql"])
        return self._get(file_path)
    
    def get_config(self, method, path):
            
        input_path = os.path.join(*[self._root_path, path, method + ".input"])        
        input_config = self._get_config(input_path)

        output_path = os.path.join(*[self._root_path, path, method + ".output"])        
        output_config = self._get_config(output_path)

        if input_config is None and output_config is None:
            return None 

        return { "input.model" : input_config, "output.model" : output_config }

    def list_sql(self, method, path):
        try:     
            files = os.listdir(os.path.join(*[self._root_path, path]))
            ffiles = [f.replace(".sql", "") for f in files if f.startswith(method) and f.endswith(".sql")]        
        except:
            ffiles = None
        return ffiles
        
    def _get_config(self, filepath):
        yaml_path = filepath + ".yaml"
        if os.path.exists(yaml_path):
            config_str = self._get(yaml_path)
            if config_str is not None and config_str != '':
                return yaml.load(config_str)
        
        json_path = filepath + ".json"
        if os.path.exists(json_path):
            config_str = self._get(json_path)
            if config_str is not None and config_str != '':
                return json.loads(config_str)

    def _get(self, file_path):        
        try:
            #print(file_path)
            with open(file_path, "r") as file:
                content = file.read()
        except:
            content = None
        return content