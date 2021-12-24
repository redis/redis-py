from functools import partial
from typing import Any, AnyStr, List, Sequence, Union

import numpy as np

from . import command_builder as builder
from .postprocessor import Processor
from deprecated import deprecated
import warnings

processor = Processor()


class Dag:
    def __init__(self, load, persist, routing, timeout, executor, readonly=False):
        self.result_processors = []
        self.enable_postprocess = True
        self.deprecatedDagrunMode = load is None and persist is None and routing is None
        self.readonly = readonly
        self.executor = executor

        if readonly and persist:
            raise RuntimeError(
                "READONLY requests cannot write (duh!) and should not "
                "have PERSISTing values"
            )

        if self.deprecatedDagrunMode:
            # Throw warning about using deprecated dagrun
            warnings.warn("Creating Dag without any of LOAD, PERSIST and ROUTING arguments"
                          "is allowed only in deprecated AI.DAGRUN or AI.DAGRUN_RO commands", DeprecationWarning)
            # Use dagrun
            if readonly:
                self.commands = ["AI.DAGRUN_RO"]
            else:
                self.commands = ["AI.DAGRUN"]
        else:
            # Use dagexecute
            if readonly:
                self.commands = ["AI.DAGEXECUTE_RO"]
            else:
                self.commands = ["AI.DAGEXECUTE"]
        if load is not None:
            if not isinstance(load, (list, tuple)):
                self.commands += ["LOAD", 1, load]
            else:
                self.commands += ["LOAD", len(load), *load]
        if persist is not None:
            if not isinstance(persist, (list, tuple)):
                self.commands += ["PERSIST", 1, persist]
            else:
                self.commands += ["PERSIST", len(persist), *persist]
        if routing is not None:
            self.commands += ["ROUTING", routing]
        if timeout is not None:
            self.commands += ["TIMEOUT", timeout]

        self.commands.append("|>")

    def tensorset(
        self,
        key: AnyStr,
        tensor: Union[np.ndarray, list, tuple],
        shape: Sequence[int] = None,
        dtype: str = None,
    ) -> Any:
        args = builder.tensorset(key, tensor, shape, dtype)
        self.commands.extend(args)
        self.commands.append("|>")
        self.result_processors.append(bytes.decode)
        return self

    def tensorget(
        self,
        key: AnyStr,
        as_numpy: bool = True,
        as_numpy_mutable: bool = False,
        meta_only: bool = False,
    ) -> Any:
        args = builder.tensorget(key, as_numpy, as_numpy_mutable)
        self.commands.extend(args)
        self.commands.append("|>")
        self.result_processors.append(
            partial(
                processor.tensorget,
                as_numpy=as_numpy,
                as_numpy_mutable=as_numpy_mutable,
                meta_only=meta_only,
            )
        )
        return self

    @deprecated(version="1.2.0", reason="Use modelexecute instead")
    def modelrun(
            self,
            key: AnyStr,
            inputs: Union[AnyStr, List[AnyStr]],
            outputs: Union[AnyStr, List[AnyStr]],
    ) -> Any:
        if self.deprecatedDagrunMode:
            args = builder.modelrun(key, inputs, outputs)
            self.commands.extend(args)
            self.commands.append("|>")
            self.result_processors.append(bytes.decode)
            return self
        else:
            return self.modelexecute(key, inputs, outputs)

    def modelexecute(
        self,
        key: AnyStr,
        inputs: Union[AnyStr, List[AnyStr]],
        outputs: Union[AnyStr, List[AnyStr]],
    ) -> Any:
        if self.deprecatedDagrunMode:
            raise RuntimeError(
                "You are using deprecated version of DAG, that does not supports MODELEXECUTE."
                "The new version requires giving at least one of LOAD, PERSIST and ROUTING"
                "arguments when constructing the Dag"
            )
        args = builder.modelexecute(key, inputs, outputs, None)
        self.commands.extend(args)
        self.commands.append("|>")
        self.result_processors.append(bytes.decode)
        return self

    def scriptexecute(
        self,
        key: AnyStr,
        function: str,
        keys: Union[AnyStr, Sequence[AnyStr]] = None,
        inputs: Union[AnyStr, Sequence[AnyStr]] = None,
        args: Union[AnyStr, Sequence[AnyStr]] = None,
        outputs: Union[AnyStr, List[AnyStr]] = None,
    ) -> Any:
        if self.readonly:
            raise RuntimeError(
                "AI.SCRIPTEXECUTE cannot be used in readonly mode"
            )
        if self.deprecatedDagrunMode:
            raise RuntimeError(
                "You are using deprecated version of DAG, that does not supports SCRIPTEXECUTE."
                "The new version requires giving at least one of LOAD, PERSIST and ROUTING"
                "arguments when constructing the Dag"
            )
        args = builder.scriptexecute(key, function, keys, inputs, args, outputs, None)
        self.commands.extend(args)
        self.commands.append("|>")
        self.result_processors.append(bytes.decode)
        return self

    @deprecated(version="1.2.0", reason="Use execute instead")
    def run(self):
        return self.execute()

    def execute(self):
        commands = self.commands[:-1]  # removing the last "|>"
        results = self.executor(*commands)
        if self.enable_postprocess:
            out = []
            for res, fn in zip(results, self.result_processors):
                out.append(fn(res))
        else:
            out = results
        return out
