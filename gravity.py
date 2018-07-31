from nodedescriptor import NodeDescriptor
from contentreader import FileReader
from nodedescriptor import NodeDescritporBuilder
from nodedescriptor import NodeDescritporFactory
from executioncontext import SQLiteExecutionContext

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
    gravity = Gravity(GravityConfiguration("/home/kirubasankars/workspace/gravity/serve"))
    execution_context = SQLiteExecutionContext()
    descriptor = gravity.create_descriptor("get", "app/api/product")
    if descriptor is not None:        
        executor = descriptor.create_executor(execution_context)
        input_shape = executor.create_input_shape({ "id": 1 })
        print(executor.get_result_json(input_shape))
