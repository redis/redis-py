from typing import AnyStr, ByteString, List, Sequence, Union

import numpy as np

from . import utils

# TODO: mypy check


def loadbackend(identifier: AnyStr, path: AnyStr) -> Sequence:
    return "AI.CONFIG LOADBACKEND", identifier, path


def modelstore(
    name: AnyStr,
    backend: str,
    device: str,
    data: ByteString,
    batch: int,
    minbatch: int,
    minbatchtimeout: int,
    tag: AnyStr,
    inputs: Union[AnyStr, List[AnyStr]],
    outputs: Union[AnyStr, List[AnyStr]],
) -> Sequence:
    if name is None:
        raise ValueError("Model name was not given")

    # device format should be: "CPU | GPU [:<num>]"
    device_type = device.split(":")[0]
    if device_type.upper() not in utils.allowed_devices:
        raise ValueError(f"Device not allowed. Use any from {utils.allowed_devices}")
    if backend.upper() not in utils.allowed_backends:
        raise ValueError(f"Backend not allowed. Use any from {utils.allowed_backends}")
    args = ["AI.MODELSTORE", name, backend, device]

    if tag is not None:
        args += ["TAG", tag]
    if batch is not None:
        args += ["BATCHSIZE", batch]
    if minbatch is not None:
        if batch is None:
            raise ValueError("Minbatch is not allowed without batch")
        args += ["MINBATCHSIZE", minbatch]
    if minbatchtimeout is not None:
        if minbatch is None:
            raise ValueError("Minbatchtimeout is not allowed without minbatch")
        args += ["MINBATCHTIMEOUT", minbatchtimeout]

    if backend.upper() == "TF":
        if not all((inputs, outputs)):
            raise ValueError(
                "Require keyword arguments inputs and outputs for TF models"
            )
        args += [
            "INPUTS",
            len(inputs) if isinstance(inputs, List) else 1,
            *utils.listify(inputs),
        ]
        args += [
            "OUTPUTS",
            len(outputs) if isinstance(outputs, List) else 1,
            *utils.listify(outputs),
        ]
    elif inputs is not None or outputs is not None:
        raise ValueError(
            "Inputs and outputs keywords should not be specified for this backend"
        )
    chunk_size = 500 * 1024 * 1024  # TODO: this should be configurable.
    data_chunks = [data[i: i + chunk_size] for i in range(0, len(data), chunk_size)]
    # TODO: need a test case for this
    args += ["BLOB", *data_chunks]
    return args


def modelset(
    name: AnyStr,
    backend: str,
    device: str,
    data: ByteString,
    batch: int,
    minbatch: int,
    tag: AnyStr,
    inputs: Union[AnyStr, List[AnyStr]],
    outputs: Union[AnyStr, List[AnyStr]],
) -> Sequence:
    if device.upper() not in utils.allowed_devices:
        raise ValueError(f"Device not allowed. Use any from {utils.allowed_devices}")
    if backend.upper() not in utils.allowed_backends:
        raise ValueError(f"Backend not allowed. Use any from {utils.allowed_backends}")
    args = ["AI.MODELSET", name, backend, device]

    if tag is not None:
        args += ["TAG", tag]
    if batch is not None:
        args += ["BATCHSIZE", batch]
    if minbatch is not None:
        if batch is None:
            raise ValueError("Minbatch is not allowed without batch")
        args += ["MINBATCHSIZE", minbatch]

    if backend.upper() == "TF":
        if not (all((inputs, outputs))):
            raise ValueError("Require keyword arguments input and output for TF models")
        args += ["INPUTS", *utils.listify(inputs)]
        args += ["OUTPUTS", *utils.listify(outputs)]
    chunk_size = 500 * 1024 * 1024
    data_chunks = [data[i: i + chunk_size] for i in range(0, len(data), chunk_size)]
    # TODO: need a test case for this
    args += ["BLOB", *data_chunks]
    return args


def modelget(name: AnyStr, meta_only=False) -> Sequence:
    args = ["AI.MODELGET", name, "META"]
    if not meta_only:
        args.append("BLOB")
    return args


def modeldel(name: AnyStr) -> Sequence:
    return "AI.MODELDEL", name


def modelexecute(
    name: AnyStr,
    inputs: Union[AnyStr, List[AnyStr]],
    outputs: Union[AnyStr, List[AnyStr]],
    timeout: int,
) -> Sequence:
    if name is None or inputs is None or outputs is None:
        raise ValueError("Missing required arguments for model execute command")
    args = [
        "AI.MODELEXECUTE",
        name,
        "INPUTS",
        len(utils.listify(inputs)),
        *utils.listify(inputs),
        "OUTPUTS",
        len(utils.listify(outputs)),
        *utils.listify(outputs),
    ]
    if timeout is not None:
        args += ["TIMEOUT", timeout]
    return args


def modelrun(
    name: AnyStr,
    inputs: Union[AnyStr, List[AnyStr]],
    outputs: Union[AnyStr, List[AnyStr]],
) -> Sequence:
    args = (
        "AI.MODELRUN",
        name,
        "INPUTS",
        *utils.listify(inputs),
        "OUTPUTS",
        *utils.listify(outputs),
    )
    return args


def modelscan() -> Sequence:
    return ("AI._MODELSCAN",)


def tensorset(
    key: AnyStr,
    tensor: Union[np.ndarray, list, tuple],
    shape: Sequence[int] = None,
    dtype: str = None,
) -> Sequence:
    if np and isinstance(tensor, np.ndarray):
        dtype, shape, blob = utils.numpy2blob(tensor)
        args = ["AI.TENSORSET", key, dtype, *shape, "BLOB", blob]
    elif isinstance(tensor, (list, tuple)):
        try:
            # Numpy 'str' dtype has many different names regarding maximal length in the tensor and more,
            # but the all share the 'num' attribute. This is a way to check if a dtype is a kind of string.
            if np.dtype(dtype).num == np.dtype("str").num:
                dtype = utils.dtype_dict["str"]
            else:
                dtype = utils.dtype_dict[dtype.lower()]
        except KeyError:
            raise TypeError(
                f"``{dtype}`` is not supported by RedisAI. Currently "
                f"supported types are {list(utils.dtype_dict.keys())}"
            )
        except AttributeError:
            raise TypeError(
                "tensorset() missing argument 'dtype' or value of 'dtype' is None"
            )
        if shape is None:
            shape = (len(tensor),)
        args = ["AI.TENSORSET", key, dtype, *shape, "VALUES", *tensor]
    else:
        raise TypeError(
            f"``tensor`` argument must be a numpy array or a list or a "
            f"tuple, but got {type(tensor)}"
        )
    return args


def tensorget(key: AnyStr, as_numpy: bool = True, meta_only: bool = False) -> Sequence:
    args = ["AI.TENSORGET", key, "META"]
    if not meta_only:
        if as_numpy is True:
            args.append("BLOB")
        else:
            args.append("VALUES")
    return args


def scriptstore(
        name: AnyStr,
        device: str,
        script: str,
        entry_points: Union[str, Sequence[str]],
        tag: AnyStr = None
) -> Sequence:
    if device.upper() not in utils.allowed_devices:
        raise ValueError(f"Device not allowed. Use any from {utils.allowed_devices}")
    if name is None or script is None or entry_points is None:
        raise ValueError("Missing required arguments for script store command")
    args = ["AI.SCRIPTSTORE", name, device]
    if tag:
        args += ["TAG", tag]
    args += ["ENTRY_POINTS", len(utils.listify(entry_points)), *utils.listify(entry_points)]
    args.append("SOURCE")
    args.append(script)
    return args


def scriptset(name: AnyStr, device: str, script: str, tag: AnyStr = None) -> Sequence:
    if device.upper() not in utils.allowed_devices:
        raise ValueError(f"Device not allowed. Use any from {utils.allowed_devices}")
    args = ["AI.SCRIPTSET", name, device]
    if tag:
        args += ["TAG", tag]
    args.append("SOURCE")
    args.append(script)
    return args


def scriptget(name: AnyStr, meta_only=False) -> Sequence:
    args = ["AI.SCRIPTGET", name, "META"]
    if not meta_only:
        args.append("SOURCE")
    return args


def scriptdel(name: AnyStr) -> Sequence:
    return "AI.SCRIPTDEL", name


def scriptrun(
    name: AnyStr,
    function: str,
    inputs: Union[AnyStr, Sequence[AnyStr]],
    outputs: Union[AnyStr, Sequence[AnyStr]],
) -> Sequence:
    if name is None or function is None:
        raise ValueError("Missing required arguments for script run command")
    args = (
        "AI.SCRIPTRUN",
        name,
        function,
        "INPUTS",
        *utils.listify(inputs),
        "OUTPUTS",
        *utils.listify(outputs),
    )
    return args


def scriptexecute(
    name: AnyStr,
    function: str,
    keys: Union[AnyStr, Sequence[AnyStr]],
    inputs: Union[AnyStr, Sequence[AnyStr]],
    input_args: Union[AnyStr, Sequence[AnyStr]],
    outputs: Union[AnyStr, Sequence[AnyStr]],
    timeout: int,
) -> Sequence:
    if name is None or function is None or (keys is None and inputs is None):
        raise ValueError("Missing required arguments for script execute command")
    args = ["AI.SCRIPTEXECUTE", name, function]

    if keys is not None:
        args += ["KEYS", len(utils.listify(keys)), *utils.listify(keys)]
    if inputs is not None:
        args += ["INPUTS", len(utils.listify(inputs)), *utils.listify(inputs)]
    if input_args is not None:
        args += ["ARGS", len(utils.listify(input_args)), *utils.listify(input_args)]
    if outputs is not None:
        args += ["OUTPUTS", len(utils.listify(outputs)), *utils.listify(outputs)]
    if timeout is not None:
        args += ["TIMEOUT", timeout]

    return args


def scriptscan() -> Sequence:
    return ("AI._SCRIPTSCAN",)


def infoget(key: AnyStr) -> Sequence:
    return "AI.INFO", key


def inforeset(key: AnyStr) -> Sequence:
    return "AI.INFO", key, "RESETSTAT"
