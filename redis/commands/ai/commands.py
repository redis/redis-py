import warnings

import numpy as np
from deprecated import deprecated
from typing import AnyStr, ByteString, List, Sequence, Union

from . import command_builder as builder
from .dag import Dag
# from .postprocessor import Processor

# processor = Processor()


class AICommands:
    def dag(
        self,
        load: Sequence = None,
        persist: Sequence = None,
        routing: AnyStr = None,
        timeout: int = None,
        readonly: bool = False
    ) -> "Dag":
        """
        It returns a DAG object on which other DAG-allowed operations can be called. For
        more details about DAG in RedisAI, refer to the RedisAI documentation.

        Parameters
        ----------
        load : Union[AnyStr, List[AnyStr]]
            Load the list of given values from the keyspace to DAG scope
        persist : Union[AnyStr, List[AnyStr]]
            For each tensor key in the given list, write its values to the keyspace from
            DAG scope after the DAG execution is finished
        routing : AnyStr
            Denotes a key to be used in the DAG or a tag that will assist in routing
            the dag execution command to the right shard. Redis will verify that all
            potential key accesses are done to within the target shard
        timeout : int
            The max number on milisecinds that may pass before the request is prossced
            (meaning that the result will not be computed after that time and TIMEDOUT
            is returned in that case)
        readonly : bool
            If True, it triggers AI.DAGRUN_RO, the read only DAG which cannot write
            (PERSIST) to the keyspace. But since it can't write, it can execute on
            replicas


        Returns
        -------
        Any
            Dag object which holds other operations permitted inside DAG as attributes

        Example
        -------
        >>> con.tensorset('tensor', ...)
        'OK'
        >>> con.modelstore('model', ...)
        'OK'
        >>> dag = con.dag(load=['tensor'], persist=['output'])
        >>> dag.tensorset('another', ...)
        >>> dag.modelrun('model', inputs=['tensor', 'another'], outputs=['output'])
        >>> output = dag.tensorget('output').run()
        >>> # You can even chain the operations
        >>> result = dag.tensorset(**akwargs)
                        .modelrun(**bkwargs)
                        .tensorget(**ckwargs).run()
        """
        return Dag(load, persist, routing, timeout, self.execute_command, readonly)

    def loadbackend(self, identifier: AnyStr, path: AnyStr) -> str:
        """
        RedisAI by default won't load any backends. User can either explicitly
        load the backend by using this function or let RedisAI load the required
        backend from the default path on-demand.

        Parameters
        ----------
        identifier : str
            Representing which backend. Allowed values - TF, TFLITE, TORCH & ONNX
        path: str
            Path to the shared object of the backend

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.loadbackend('TORCH', '/path/to/the/backend/redisai_torch.so')
        'OK'
        """
        args = builder.loadbackend(identifier, path)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.loadbackend(res)

    def modelstore(
        self,
        key: AnyStr,
        backend: str,
        device: str,
        data: ByteString,
        batch: int = None,
        minbatch: int = None,
        minbatchtimeout: int = None,
        tag: AnyStr = None,
        inputs: Union[AnyStr, List[AnyStr]] = None,
        outputs: Union[AnyStr, List[AnyStr]] = None,
    ) -> str:
        """
        Set the model on provided key.

        Parameters
        ----------
        key : AnyStr
            Key name
        backend : str
            Backend name. Allowed backends are TF, TORCH, TFLITE, ONNX
        device : str
            Device name. Allowed devices are CPU and GPU. If multiple GPUs are available
            it can be specified using the format GPU:<gpu number>. For example: GPU:0
        data : bytes
            Model graph read as bytes string
        batch : int
            Number of batches for doing auto-batching
        minbatch : int
            Minimum number of samples required in a batch for model execution
        minbatchtimeout : int
            The max number of miliseconds for which the engine will not trigger
            an execution if the number of samples is lower than minbatch (after
            minbatchtimeout is passed, the execution will start even if minbatch
            jas not reached)
        tag : AnyStr
            Any string that will be saved in RedisAI as tag for the model
        inputs : Union[AnyStr, List[AnyStr]]
            Input node(s) in the graph. Required only Tensorflow graphs
        outputs : Union[AnyStr, List[AnyStr]]
            Output node(s) in the graph Required only for Tensorflow graphs

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> # Torch model
        >>> model_path = os.path.join('path/to/TorchScriptModel.pt')
        >>> model = open(model_path, 'rb').read()
        >>> con.modeltore("model", 'torch', 'cpu', model, tag='v1.0')
        'OK'
        >>> # Tensorflow model
        >>> model_path = os.path.join('/path/to/tf_frozen_graph.pb')
        >>> model = open(model_path, 'rb').read()
        >>> con.modelstore('m', 'tf', 'cpu', model,
        ...              inputs=['a', 'b'], outputs=['mul'], tag='v1.0')
        'OK'
        """
        args = builder.modelstore(
            key,
            backend,
            device,
            data,
            batch,
            minbatch,
            minbatchtimeout,
            tag,
            inputs,
            outputs,
        )
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelstore(res)

    @deprecated(version="1.2.0", reason="Use modelstore instead")
    def modelset(
        self,
        key: AnyStr,
        backend: str,
        device: str,
        data: ByteString,
        batch: int = None,
        minbatch: int = None,
        tag: AnyStr = None,
        inputs: Union[AnyStr, List[AnyStr]] = None,
        outputs: Union[AnyStr, List[AnyStr]] = None,
    ) -> str:
        """
        Set the model on provided key.

        Parameters
        ----------
        key : AnyStr
            Key name
        backend : str
            Backend name. Allowed backends are TF, TORCH, TFLITE, ONNX
        device : str
            Device name. Allowed devices are CPU and GPU. If multiple GPUs are available
            it can be specified using the format GPU:<gpu number>. For example: GPU:0
        data : bytes
            Model graph read as bytes string
        batch : int
            Number of batches for doing auto-batching
        minbatch : int
            Minimum number of samples required in a batch for model execution
        tag : AnyStr
            Any string that will be saved in RedisAI as tag for the model
        inputs : Union[AnyStr, List[AnyStr]]
            Input node(s) in the graph. Required only Tensorflow graphs
        outputs : Union[AnyStr, List[AnyStr]]
            Output node(s) in the graph Required only for Tensorflow graphs

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> # Torch model
        >>> model_path = os.path.join('path/to/TorchScriptModel.pt')
        >>> model = open(model_path, 'rb').read()
        >>> con.modelset("model", 'torch', 'cpu', model, tag='v1.0')
        'OK'
        >>> # Tensorflow model
        >>> model_path = os.path.join('/path/to/tf_frozen_graph.pb')
        >>> model = open(model_path, 'rb').read()
        >>> con.modelset('m', 'tf', 'cpu', model,
        ...              inputs=['a', 'b'], outputs=['mul'], tag='v1.0')
        'OK'
        """
        args = builder.modelset(
            key, backend, device, data, batch, minbatch, tag, inputs, outputs
        )
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelset(res)

    def modelget(self, key: AnyStr, meta_only=False) -> dict:
        """
        Fetch the model details and the model blob back from RedisAI

        Parameters
        ----------
        key : AnyStr
            Model key in RedisAI
        meta_only : bool
            If True, only the meta data will be fetched, not the model blob

        Returns
        -------
        dict
            A dictionary of model details such as device, backend etc. The model
            blob will be available at the key 'blob'

        Example
        -------
        >>> con.modelget('model', meta_only=True)
        {'backend': 'TF', 'device': 'cpu', 'tag': 'v1.0'}
        """
        args = builder.modelget(key, meta_only)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelget(res)

    def modeldel(self, key: AnyStr) -> str:
        """
        Delete the model from the RedisAI server

        Parameters
        ----------
        key : AnyStr
            Key of the model to be deleted

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.modeldel('model')
        'OK'
        """
        args = builder.modeldel(key)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modeldel(res)

    def modelexecute(
        self,
        key: AnyStr,
        inputs: Union[AnyStr, List[AnyStr]],
        outputs: Union[AnyStr, List[AnyStr]],
        timeout: int = None,
    ) -> str:
        """
        Run the model using input(s) which are already in the scope and are associated
        to some keys. Modelexecute also needs the output key name(s) to store the output
        from the model. The number of outputs from the model and the number of keys
        provided here must be same. Otherwise, RedisAI throws an error

        Parameters
        ----------
        key : str
            Model key to run
        inputs : Union[AnyStr, List[AnyStr]]
            Tensor(s) which is already saved in the RedisAI using a tensorset call.
            These tensors will be used as the inputs for the modelexecute
        outputs : Union[AnyStr, List[AnyStr]]
            keys on which the outputs to be saved. If those keys exist already,
            modelexecute will overwrite them with new values
        timeout : int
            The max number on milisecinds that may pass before the request is prossced
            (meaning that the result will not be computed after that time and TIMEDOUT
            is returned in that case

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.modelstore('m', 'tf', 'cpu', model_pb,
        ...              inputs=['a', 'b'], outputs=['mul'], tag='v1.0')
        'OK'
        >>> con.tensorset('a', (2, 3), dtype='float')
        'OK'
        >>> con.tensorset('b', (2, 3), dtype='float')
        'OK'
        >>> con.modelexecute('m', ['a', 'b'], ['c'])
        'OK'
        """
        args = builder.modelexecute(key, inputs, outputs, timeout)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelexecute(res)

    @deprecated(version="1.2.0", reason="Use modelexecute instead")
    def modelrun(
        self,
        key: AnyStr,
        inputs: Union[AnyStr, List[AnyStr]],
        outputs: Union[AnyStr, List[AnyStr]],
    ) -> str:
        """
        Run the model using input(s) which are already in the scope and are associated
        to some keys. Modelrun also needs the output key name(s) to store the output
        from the model. The number of outputs from the model and the number of keys
        provided here must be same. Otherwise, RedisAI throws an error

        Parameters
        ----------
        key : str
            Model key to run
        inputs : Union[AnyStr, List[AnyStr]]
            Tensor(s) which is already saved in the RedisAI using a tensorset call.
            These tensors will be used as the input for the modelrun
        outputs : Union[AnyStr, List[AnyStr]]
            keys on which the outputs to be saved. If those keys exist already, modelrun
            will overwrite them with new values

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.modelstore('m', 'tf', 'cpu', model_pb,
        ...              inputs=['a', 'b'], outputs=['mul'], tag='v1.0')
        'OK'
        >>> con.tensorset('a', (2, 3), dtype='float')
        'OK'
        >>> con.tensorset('b', (2, 3), dtype='float')
        'OK'
        >>> con.modelrun('m', ['a', 'b'], ['c'])
        'OK'
        """
        args = builder.modelrun(key, inputs, outputs)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelrun(res)

    def modelscan(self) -> List[List[AnyStr]]:
        """
        Returns the list of all the models in the RedisAI server. Modelscan API is
        currently experimental and might be removed or changed in the future without
        warning

        Returns
        -------
        List[List[AnyStr]]
            List of list of models and tags for each model if they existed

        Example
        -------
        >>> con.modelscan()
        [['pt_model', ''], ['m', 'v1.2']]
        """
        warnings.warn(
            "Experimental: Model List API is experimental and might change "
            "in the future without any notice",
            UserWarning,
        )
        args = builder.modelscan()
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.modelscan(res)

    def tensorset(
        self,
        key: AnyStr,
        tensor: Union[np.ndarray, list, tuple],
        shape: Sequence[int] = None,
        dtype: str = None,
    ) -> str:
        """
        Set the tensor to a key in RedisAI

        Parameters
        ----------
        key : AnyStr
            The name of the tensor
        tensor : Union[np.ndarray, list, tuple]
            A `np.ndarray` object or Python list or tuple
        shape : Sequence[int]
            Shape of the tensor. Required if `tensor` is list or tuple
        dtype : str
            Data type of the tensor. Required if `tensor` is list or tuple

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.tensorset('a', (2, 3), dtype='float')
        'OK'
        >>> input_array = np.array([2, 3], dtype=np.float32)
        >>> con.tensorset('x', input_array)
        'OK'
        """
        args = builder.tensorset(key, tensor, shape, dtype)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.tensorset(res)

    def tensorget(
        self,
        key: AnyStr,
        as_numpy: bool = True,
        as_numpy_mutable: bool = False,
        meta_only: bool = False,
    ) -> Union[dict, np.ndarray]:
        """
        Retrieve the value of a tensor from the server. By default it returns the numpy
        array but it can be controlled using the `as_type` and `meta_only` argument.

        Parameters
        ----------
        key : AnyStr
            The name of the tensor
        as_numpy : bool
            If True, returns a numpy.ndarray. Returns the value as a list and the
            metadata in a dictionary if False. This flag also decides how to fetch
            the value from the RedisAI server, which also has performance implications
        as_numpy_mutable : bool
            If True, returns a a mutable numpy.ndarray object by copy the tensor data.
            Otherwise (as long as_numpy=True) the returned numpy.ndarray will use the
            original tensor buffer and will be for read-only
        meta_only : bool
            If True, the value is not retrieved, only the shape and the type

        Returns
        -------
        Union[dict, np.ndarray]
            Returns a dictionary of data or a numpy array. Default is numpy array

        Example
        -------
        >>> con.tensorget('x')
        array([2, 3, 4])
        >>> con.tensorget('x' as_numpy=False)
        {'values': [2, 3, 4], 'dtype': 'INT64', 'shape': [3]}
        >>> con.tensorget('x', meta_only=True)
        {'dtype': 'INT64', 'shape': [3]}
        """
        args = builder.tensorget(key, as_numpy, meta_only)
        options = {
            "as_numpy": as_numpy,
            "as_numpy_mutable": as_numpy_mutable,
            "meta_only": meta_only
        }
        res = self.execute_command(*args, **options)
        return (
            res
            # if not self.enable_postprocess
            # else processor.tensorget(res, as_numpy, as_numpy_mutable, meta_only)
        )

    def scriptstore(
        self,
        key: AnyStr,
        device: str,
        script: str,
        entry_points: Union[str, Sequence[str]],
        tag: AnyStr = None
    ) -> str:
        """
        Set the script to RedisAI. The difference from scriptset is that in scriptstore
        you must specify entry points within your script. These functions must have specific
        signature: 'def entry_point(tensors: List[Tensor], keys: List[str], args: List[str])'. # noqa
        RedisAI uses the TorchScript engine to execute the script. So the script should
        have only TorchScript supported constructs. That being said, it's important to
        mention that using redisai script to do post processing or pre processing for a
        Tensorflow (or any other backend) is completely valid. For more details about
        TorchScript and supported ops, checkout TorchScript documentation.

        Parameters
        ----------
        key : AnyStr
            Script key at the server
        device : str
            Device name. Allowed devices are CPU and GPU. If multiple GPUs are available
            it can be specified using the format GPU:<gpu number>. For example: GPU:0
        script : str
            Script itself, as a Python string
        entry_points : Union[str, Sequence[str]]
            A list of functions in the script that may serve as entry point for the
            execution. Each entry point must have the specify signature:
            def entry_point(tensors: List[Tensor], keys: List[str], args: List[str]))
            Note that the script may contain additional helper functions that doesn't
            have to follow this signature.
        tag : AnyStr
            Any string that will be saved in RedisAI as tag for the script

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Note
        ----
        Even though ``script`` is pure Python code, it's a subset of Python
        language and not all the Python operations are supported. For more
        details, checkout TorchScript documentation. It's also important to
        note that that the script is executed on a high performance C++ runtime
        instead of the Python interpreter. And hence ``script`` should not have
        any import statements (A common mistake people make all the time)

        Example
        -------
        >>> script = r'''
        >>> def bar(tensors: List[Tensor], keys: List[str], args: List[str]):
        >>>     a = tensors[0]
        >>>     b = tensors[1]
        >>>     return a + b
        >>>'''
        >>> con.scriptstore('ket', 'cpu', script, 'bar')
        'OK'
        """
        args = builder.scriptstore(key, device, script, entry_points, tag)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptstore(res)

    @deprecated(version="1.2.0", reason="Use scriptstore instead")
    def scriptset(
        self, key: AnyStr, device: str, script: str, tag: AnyStr = None
    ) -> str:
        """
        Set the script to RedisAI. Action similar to Modelset. RedisAI uses the
        TorchScript engine to execute the script. So the script should have only
        TorchScript supported constructs. That being said, it's important to
        mention that using redisai script to do post processing or pre processing
        for a Tensorflow (or any other backend) is completely valid. For more
        details about TorchScript and supported ops, checkout TorchScript
        documentation

        Parameters
        ----------
        key : AnyStr
            Script key at the server
        device : str
            Device name. Allowed devices are CPU and GPU. If multiple GPUs are available
            it can be specified using the format GPU:<gpu number>. For example: GPU:0
        script : str
            Script itself, as a Python string
        tag : AnyStr
            Any string that will be saved in RedisAI as tag for the model

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Note
        ----
        Even though ``script`` is pure Python code, it's a subset of Python
        language and not all the Python operations are supported. For more
        details, checkout TorchScript documentation. It's also important to
        note that that the script is executed on a high performance C++ runtime
        instead of the Python interpreter. And hence ``script`` should not have
        any import statements (A common mistake people make all the time)

        Example
        -------
        >>> script = open(scriptpath).read()
        >>> con.scriptset('ket', 'cpu', script)
        'OK'
        """
        args = builder.scriptset(key, device, script, tag)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptset(res)

    def scriptget(self, key: AnyStr, meta_only=False) -> dict:
        """
        Get the saved script from RedisAI. Operation similar to model get

        Parameters
        ----------
        key : AnyStr
            Key of the script
        meta_only : bool
            If True, only the meta data will be fetched, not the script itself

        Returns
        -------
        dict
            Dictionary of script details which includes the script at the key ``source``

        Example
        -------
        >>> con.scriptget('ket', meta_only=True)
        {'device': 'cpu'}
        """
        args = builder.scriptget(key, meta_only)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptget(res)

    def scriptdel(self, key: AnyStr) -> str:
        """
        Delete the script from the RedisAI server

        Parameters
        ----------
        key : AnyStr
            Script key to be deleted

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.scriptdel('ket')
        'OK'
        """
        args = builder.scriptdel(key)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptdel(res)

    @deprecated(version="1.2.0", reason="Use scriptexecute instead")
    def scriptrun(
        self,
        key: AnyStr,
        function: str,
        inputs: Union[AnyStr, Sequence[AnyStr]],
        outputs: Union[AnyStr, Sequence[AnyStr]],
    ) -> str:
        """
        Run an already set script. Similar to modelrun

        Parameters
        ----------
        key : AnyStr
            Script key
        function : str
            Name of the function in the ``script``
        inputs : Union[AnyStr, List[AnyStr]]
            Tensor(s) which is already saved in the RedisAI using a tensorset
            call. These tensors will be used as the input for the modelrun
        outputs : Union[AnyStr, List[AnyStr]]
            keys on which the outputs to be saved. If those keys exist already,
            scriptrun will overwrite them with new values

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.scriptrun('ket', 'bar', inputs=['a', 'b'], outputs=['c'])
        'OK'
        """
        args = builder.scriptrun(key, function, inputs, outputs)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptrun(res)

    def scriptexecute(
        self,
        key: AnyStr,
        function: str,
        keys: Union[AnyStr, Sequence[AnyStr]] = None,
        inputs: Union[AnyStr, Sequence[AnyStr]] = None,
        args: Union[AnyStr, Sequence[AnyStr]] = None,
        outputs: Union[AnyStr, Sequence[AnyStr]] = None,
        timeout: int = None,
    ) -> str:
        """
        Run an already set script. Similar to modelexecute.
        Must specify keys or inputs.

        Parameters
        ----------
        key : AnyStr
            Script key
        function : str
            Name of the function in the ``script``
        keys : Union[AnyStr, Sequence[AnyStr]]
            Denotes the list of Redis key names that the script will access to
            during its execution, for both read and/or write operations.
        inputs : Union[AnyStr, Sequence[AnyStr]]
            Denotes the input tensors list.
        args : Union[AnyStr, Sequence[AnyStr]]
            Denotes the list of additional arguments that a user can send to the
            script. All args are sent as strings, but can be casted to other types
            supported by torch script, such as int, or float.
        outputs : Union[AnyStr, List[AnyStr]]
            Denotes the output tensors keys' list. If those keys exist already,
            scriptexecute will overwrite them with new values.
        timeout : int
            The max number on milisecinds that may pass before the request is prossced
            (meaning that the result will not be computed after that time and TIMEDOUT
            is returned in that case).

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.scriptexecute('myscript', 'bar', inputs=['a', 'b'], outputs=['c'])
        'OK'
        >>> con.scriptexecute('myscript{tag}', 'addn',
        >>>                   inputs=['tensor1{tag}', 'tensor2{tag}', 'tensor3{tag}'],
        >>>                   args=['5.0'],
        >>>                   outputs=['result{tag}'])
        'OK'
        """
        args = builder.scriptexecute(key, function, keys, inputs, args,
                                     outputs, timeout)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptexecute(res)

    def scriptscan(self) -> List[List[AnyStr]]:
        """
        Returns the list of all the script in the RedisAI server. Scriptscan API is
        currently experimental and might remove or change in the future without warning

        Returns
        -------
        List[List[AnyStr]]
            List of list of scripts and tags for each script if they existed

        Example
        -------
        >>> con.scriptscan()
        [['ket1', 'v1.0'], ['ket2', '']]
        """
        warnings.warn(
            "Experimental: Script List API is experimental and might change "
            "in the future without any notice",
            UserWarning,
        )
        args = builder.scriptscan()
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.scriptscan(res)

    def infoget(self, key: AnyStr) -> dict:
        """
        Get information such as
        - How long since the model has been running
        - How many samples have been processed
        - How many calls handled
        - How many errors raised
        - etc.

        Parameters
        ----------
        key : AnyStr
            Model key

        Returns
        -------
        dict
            Dictionary of model run details

        Example
        -------
        >>> con.infoget('m')
        {'key': 'm', 'type': 'MODEL', 'backend': 'TF', 'device': 'cpu', 'tag': '',
        'duration': 0, 'samples': 0, 'calls': 0, 'errors': 0}
        """
        args = builder.infoget(key)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.infoget(res)

    def inforeset(self, key: AnyStr) -> str:
        """
        Reset the run information about the model

        Parameters
        ----------
        key : AnyStr
            Model key

        Returns
        -------
        str
            'OK' if success, raise an exception otherwise

        Example
        -------
        >>> con.inforeset('m')
        'OK'
        """
        args = builder.inforeset(key)
        res = self.execute_command(*args)
        return res # if not self.enable_postprocess else processor.inforeset(res)
