import argparse
import os

from nodedescriptor import NodeDescriptor, NodeDescriptorBuilder, NodeDescriptorFactory
from executioncontext import SQLiteExecutionContext
from postgresqlexecutioncontext import PostgresExecutionContext
from contentreader import FileReader

class Gravity:

    def __init__(self, root_path, content_reader):
        self._descriptors = {}
        self._executors = {}
        self._root_path = root_path
        if content_reader is None:
            self._content_reader = FileReader(self._root_path)
        else:
            self._content_reader = content_reader
        self._node_descriptor_builder = NodeDescriptorBuilder(self._content_reader)
        self._node_descriptor_factory = NodeDescriptorFactory(self._content_reader, self._node_descriptor_builder)

    def create_execution_contexts(self):        
        execution_contexts = {
            "db" : PostgresExecutionContext(self._root_path, "dvdrental"),
            "sqlite3" : SQLiteExecutionContext(self._root_path, "sqlite3.db"),
            "app.db": SQLiteExecutionContext(self._root_path, "app.db")
        }
        return execution_contexts

    def create_execution_context(self, name, config):
        if name == "db":
            return PostgresExecutionContext(self._root_path, config)
        if name == "sqlite3":
            return SQLiteExecutionContext(self._root_path, config)
        if name == "app.db":
            return SQLiteExecutionContext(self._root_path, config)        
    
    def create_executor(self, method, path, debug):
        
        k = os.path.join(*[path, method])   
        if debug == False and  k in self._executors:
            return self._executors[k]
        
        descriptor = self.create_descriptor(method, path, debug)
        if descriptor is None:
            return None
        
        executor = descriptor.create_executor()
        
        self._executors[k] = executor
        
        return executor 

    def create_descriptor(self, method, path, debug):
            
        k = os.path.join(*[path, method])        
        if debug == False and  k in self._descriptors:
            return self._descriptors[k]            
        
        descriptor = self._node_descriptor_factory.create(method, path)
        self._descriptors[k] = descriptor
        return descriptor