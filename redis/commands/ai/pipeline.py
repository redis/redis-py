from functools import partial
from typing import AnyStr, Sequence, Union

import numpy as np
import redis

from . import command_builder as builder
from .postprocessor import Processor

processor = Processor()


class Pipeline(redis.client.Pipeline):
    def __init__(self, enable_postprocess, *args, **kwargs):
        self.enable_postprocess = enable_postprocess
        self.tensorget_processors = []
        self.tensorset_processors = []
        super().__init__(*args, **kwargs)

    def tensorget(self, key, as_numpy=True, as_numpy_mutable=False, meta_only=False):
        self.tensorget_processors.append(
            partial(
                processor.tensorget,
                as_numpy=as_numpy,
                as_numpy_mutable=as_numpy_mutable,
                meta_only=meta_only,
            )
        )
        args = builder.tensorget(key, as_numpy, meta_only)
        return self.execute_command(*args)

    def tensorset(
        self,
        key: AnyStr,
        tensor: Union[np.ndarray, list, tuple],
        shape: Sequence[int] = None,
        dtype: str = None,
    ) -> str:
        args = builder.tensorset(key, tensor, shape, dtype)
        return self.execute_command(*args)

    def _execute_transaction(self, *args, **kwargs):
        res = super()._execute_transaction(*args, **kwargs)
        for i in range(len(res)):
            # tensorget will have minimum 4 values if meta_only = True
            if isinstance(res[i], list) and len(res[i]) >= 4:
                res[i] = self.tensorget_processors.pop(0)(res[i])
        return res

    def _execute_pipeline(self, *args, **kwargs):
        res = super()._execute_pipeline(*args, **kwargs)
        for i in range(len(res)):
            # tensorget will have minimum 4 values if meta_only = True
            if isinstance(res[i], list) and len(res[i]) >= 4:
                res[i] = self.tensorget_processors.pop(0)(res[i])
        return res
