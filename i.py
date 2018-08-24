# importing the required module
import timeit
 
# code snippet to be executed only once
mysetup = '''
from gravity import Gravity, create_input_shape, get_result
path = "api/film"
method = "get"
root_path = "serve/pos"

app = Gravity(root_path, None)
node_descriptor = app.create_descriptor(method, path, False)
execution_contexts = app.create_execution_contexts()
input_shape = create_input_shape(node_descriptor, None, None, { "page" : 1 }, None)
'''
 
# code snippet whose execution time is to be measured
mycode = '''
get_result(node_descriptor, execution_contexts, input_shape)
'''
 
# timeit statement
print(timeit.timeit(setup = mysetup,
                    stmt = mycode,
                    number = 10))