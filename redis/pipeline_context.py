'''
This module allows you to use redis pipelines as context managers, putting the return value inside variables.
It doesn't do any special magic, or async usage of values, it just saves some code and makes code using pipelines prettier.

the basic usage is as follows:

    r = redis.Redis(...)
    
    with PipelineContext(connection = r, transaction = False) as p:
        foo = p.get('foo')
        bar = p.smembers('bar')
        
    print foo #prints the result inside foo
    
    print bar #prints the result inside bar
 
'''

import redis

        
class PipelineContextError(Exception):
    
    pass


    
class PipedResult:
    """
    This is a container class that holds the result after the pipeline has been executed.
    It has an internal return value which is set by the context manager,
    and a state telling it whether it has been initialized or not
    """
    
    
    
    #members which we always return from this instance
    __reservedMembers = set(('_doCommand_', '_setValue_', '_pipeline','_value','_state', '__bases__'))
    
    ST_NEW = 1
    ST_SENT = 2
    ST_RETURNED = 3
    
       
    def __init__(self, pipeline):
        """
        Constructor. Remember the pipeline object to extract the right call from it, and init our state
        """
        self._pipeline = pipeline
        self._value = None
        self._state = PipedResult.ST_NEW
        
    def __getattr__( self, name ):
        """
        Catchall
        """
        
        
        #if this is a reserved member, just return it
        if  name in PipedResult.__reservedMembers:
            
            return getattr( self, '__dict__')[name] 
        #if this is a command
        if self._state == PipedResult.ST_NEW:
            #make sure the pipeline has it
            if hasattr(self._pipeline, name):
                #return a lambda to be used for executing the command
                return lambda *args, **kwargs: self._doCommand_(name, *args, **kwargs)
        elif self._state == PipedResult.ST_RETURNED:
            return getattr(self._value, name)
        
        raise AttributeError("Invalid Attribute %s" % name)
        
    def _doCommand_(self, command, *args, **kwargs):
        """
        Generic command executor
        """
        
        getattr(self._pipeline, command)(*args, **kwargs)
        
        return self
    
    def _setValue_(self, value):
        """
        Used to set the internal value by the pipeline context
        """ 
        self._value = value
        self._state = PipedResult.ST_RETURNED
        
        
class PipelineContext(object):
    '''
    this class is the context manager that starts and ends transactions
    '''

    
    def __init__(self, connection = None, transaction = True):
        '''
        Constructor. This is called when the context manager starts
        @param connection the redis connection to be used. if this does not exist, we simply create a default connection
        @param transaction whether this pipeline is in transaction mode or not
        '''
        
        self._connection  = connection or redis.Redis()
        self._pipeline = self._connection.pipeline(transaction = transaction)
        self._values = []
    
    def __enter__(self):
        """
        Called when entering the context manager
        """
        return self
    
    def __exit__(self, *args):
        """
        Called when the context goes out of scope.
        We're not catching any exceptions in execute!
        """
        #execute the pipeline and get the result        
        res = self._pipeline.execute()
        
        #put the results inside the piped response objects
        for i in xrange(len(res)):
            self._values[i]._setValue_(res[i])
            
        
    def __getattribute__(self, *args, **kwargs):
        """
        Catch all that creates a new result object for each call to the pipeline
        """
        
        
        #This is a call to a pipeline command 
        if not args[0].startswith('_'):
            
            #create a delayed response object
            action = PipedResult(self._pipeline)
            #add it to the list of responses waiting for execution
            object.__getattribute__(self, ('_values')).append(action)
            #now let the response object deal with it
            return getattr(action, args[0])
        
        #this is a call to our member
        return object.__getattribute__(self, *args, **kwargs)


class Proxy:
    def __init__( self, subject ):
        self.__subject = subject
    def __getattr__( self, name ):
        return getattr( self.__subject, name ) 

if __name__ == '__main__':
    
    
    redis.Redis().set('foo', 'bar')
    with PipelineContext() as pipe:
        
        
        pipe.sadd('bar', 'baz')
        x = pipe.get('foo')
        y = pipe.smembers('bar')
        
    print x.upper()
    print len(x)
    print y
    print [c for c in x]
    print isinstance(x, str)
#        
#    
#            