import argparse

from nodedescriptor import NodeDescriptor
from nodedescriptor import NodeDescritporBuilder
from nodedescriptor import NodeDescritporFactory
from executioncontext import SQLiteExecutionContext
from postgresqlexeuctioncontext import PostgresExecutionContext
from contentreader import FileReader

class GravityConfiguration:

    def __init__(self, root_path):
        self._root_path = root_path

    def get_root_path(self):
        return self._root_path

class ExecutionContext:

    def __init__(self, name, context):
        self._name = name
        self._context = context

    def get_name(self):
        return self._name

    def get_context(self):
        return self._context

class Gravity:

    def __init__(self, gravity_configuration, content_reader):
        self._descriptors = {}
        self._executors = {}
        self._gravity_configuration = gravity_configuration
        if content_reader is None:
            self._content_reader = FileReader(gravity_configuration)
        else:
            self._content_reader = content_reader
        self._node_descriptor_builder = NodeDescritporBuilder(self._content_reader)
        self._node_descriptor_factory = NodeDescritporFactory(self._content_reader, self._node_descriptor_builder)

    def create_execution_contexts(self):
        e1 = ExecutionContext("db", PostgresExecutionContext(self._gravity_configuration, "dvdrental"))
        e2 = ExecutionContext("sqlite3", SQLiteExecutionContext(self._gravity_configuration, "app.db"))
        
        execution_contexts = {}
        for e in [ e1, e2 ]:
            execution_contexts[e.get_name()] = e.get_context()

        return execution_contexts
    
    def create_executor(self, method, path, debug):
        
        k = path + "/" + method        
        if debug == False and  k in self._executors:
            return self._executors[k]
        
        descriptor = self.create_descriptor(method, path, debug)
        if descriptor is None:
            return None
        
        executor = descriptor.create_executor()
        
        self._executors[k] = executor
        
        return executor 

    def create_descriptor(self, method, path, debug):
        
        k = path + "/" + method        
        if debug == False and  k in self._descriptors:
            return self._descriptors[k]            
        
        descriptor = self._node_descriptor_factory.create(method, path)
        self._descriptors[k] = descriptor
        return descriptor