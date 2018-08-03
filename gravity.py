import argparse

from nodedescriptor import NodeDescriptor
from nodedescriptor import NodeDescritporBuilder
from nodedescriptor import NodeDescritporFactory
from executioncontext import SQLiteExecutionContext
from contentreader import FileReader

class GravityConfiguration:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

class Gravity:

    def __init__(self, gravity_configuration):
        self._gravity_configuration = gravity_configuration
        self._content_reader = FileReader(self._gravity_configuration)
        self._node_descriptor_builder = NodeDescritporBuilder(self._content_reader)
        self._node_descriptor_factory = NodeDescritporFactory(self._content_reader, self._node_descriptor_builder)

    def create_descriptor(self, method, path):        
        return self._node_descriptor_factory.create(method, path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='path')
    parser.add_argument('--method', help='method')
    args = parser.parse_args()
    
    if args.path is None or args.method is None:
        args.path = "app/api/product"
        args.method = "get"

    gravity = Gravity(GravityConfiguration("serve"))
    execution_context = SQLiteExecutionContext()
    descriptor = gravity.create_descriptor(args.method, args.path)

    if descriptor is not None:        
        executor = descriptor.create_executor(execution_context)
        input_shape = executor.create_input_shape({ "name" : "dasd"})
        print(executor.get_result_json(input_shape))