import os
import yaml
import json

class FileReader:

    def __init__(self, gravity_configuration):
        self._gravity_configuration = gravity_configuration

    def get_sql(self, method, path):
        file_path = os.path.join(*[self._gravity_configuration.get_root_path(), path, method + ".sql"])
        return self._get(file_path)
    
    def get_config(self, method, path):
        yaml_path = os.path.join(*[self._gravity_configuration.get_root_path(), path, method + ".yml"])
        if os.path.exists(yaml_path):
            config_str = self._get(yaml_path)
            if config_str is not None and config_str != '':
                return yaml.load(config_str)
        
        json_path = os.path.join(*[self._gravity_configuration.get_root_path(), path, method + ".json"])
        if os.path.exists(json_path):
            config_str = self._get(json_path)
            if config_str is not None and config_str != '':
                return json.load(config_str)
        
        return None
        
    def _get(self, file_path):        
        try:
            #print(file_path)
            with open(file_path, "r") as file:
                content = file.read()
        except:
            content = None
        return content

    def list_sql(self, method, path):
        try:     
            files = os.listdir(os.path.join(*[self._gravity_configuration.get_root_path(), path]))
            ffiles = [f.replace(".sql", "") for f in files if f.startswith(method) and f.endswith(".sql")]        
        except:
            ffiles = None
        return ffiles
