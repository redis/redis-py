from typing import AnyStr, ByteString, Callable, List, Sequence, Union
import numpy as np

dtype_dict = {
    "float": "FLOAT",
    "double": "DOUBLE",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "int8": "INT8",
    "int16": "INT16",
    "int32": "INT32",
    "int64": "INT64",
    "uint8": "UINT8",
    "uint16": "UINT16",
    "uint32": "UINT32",
    "uint64": "UINT64",
    "bool": "BOOL",
    "str": "STRING",
}

allowed_devices = {"CPU", "GPU"}
allowed_backends = {"TF", "TFLITE", "TORCH", "ONNX"}


def numpy2blob(tensor: np.ndarray) -> tuple:
    """Convert the numpy input from user to `Tensor`."""
    try:
        if tensor.dtype.num == np.dtype("str").num:
            dtype = dtype_dict["str"]
            blob = "".join([string + "\0" for string in tensor.flat])
        else:
            dtype = dtype_dict[str(tensor.dtype)]
            blob = tensor.tobytes()
    except KeyError:
        raise TypeError(f"RedisAI doesn't support tensors of type {tensor.dtype}")
    shape = tensor.shape
    return dtype, shape, blob


def blob2numpy(
    value: ByteString, shape: Union[list, tuple], dtype: str, mutable: bool
) -> np.ndarray:
    """Convert `BLOB` result from RedisAI to `np.ndarray`."""
    mm = {"FLOAT": "float32", "DOUBLE": "float64"}
    dtype = mm.get(dtype, dtype.lower())
    if dtype == 'string':
        a = np.array(value.decode().split('\0')[:-1], dtype='str')
    elif mutable:
        a = np.fromstring(value, dtype=dtype)
    else:
        a = np.frombuffer(value, dtype=dtype)
    return a.reshape(shape)


def list2dict(lst):
    """Convert the list from RedisAI to a dict."""
    if len(lst) % 2 != 0:
        raise RuntimeError("Can't unpack the list: {}".format(lst))
    out = {}
    for i in range(0, len(lst), 2):
        key = lst[i].decode().lower()
        val = lst[i + 1]
        if key != "blob" and isinstance(val, bytes):
            val = val.decode()
        out[key] = val
    return out


def recursive_bytetransform(arr: List[AnyStr], target: Callable) -> list:
    """
    Recurse value, replacing each element of b'' with the appropriate element.

    Function returns the same array after inplace operation which updates `arr`
    """
    for ix in range(len(arr)):
        obj = arr[ix]
        if isinstance(obj, list):
            recursive_bytetransform(obj, target)
        else:
            arr[ix] = target(obj)
    return arr


def listify(inp: Union[str, Sequence[str]]) -> Sequence[str]:
    """Wrap the ``inp`` with a list if it's not a list already."""
    return (inp,) if not isinstance(inp, (list, tuple)) else inp
