from . import utils


def decoder(val):
    if isinstance(val, str):
        return val
    return val.decode()


def modelget_decode(res):
    resdict = utils.list2dict(res)
    utils.recursive_bytetransform(resdict["inputs"], lambda x: x.decode())
    utils.recursive_bytetransform(resdict["outputs"], lambda x: x.decode())
    return resdict


def modelscan_decode(res):
    return utils.recursive_bytetransform(res, lambda x: x.decode())


def tensorget_decode(res, as_numpy, as_numpy_mutable, meta_only):
    """Process the tensorget output.

    If ``as_numpy`` is True, it'll be converted to a numpy array. The required
    information such as datatype and shape must be in ``rai_result`` itself.
    """
    rai_result = utils.list2dict(res)
    if meta_only is True:
        return rai_result
    elif as_numpy_mutable is True:
        return utils.blob2numpy(
            rai_result["blob"],
            rai_result["shape"],
            rai_result["dtype"],
            mutable=True,
        )
    elif as_numpy is True:
        return utils.blob2numpy(
            rai_result["blob"],
            rai_result["shape"],
            rai_result["dtype"],
            mutable=False,
        )
    else:
        if rai_result["dtype"] == "STRING":

            def target(b):
                return b.decode()

        else:
            target = float if rai_result["dtype"] in ("FLOAT", "DOUBLE") else int
        utils.recursive_bytetransform(rai_result["values"], target)
        return rai_result


def scriptget_decode(res):
    return utils.list2dict(res)


def scriptscan_decode(res):
    return utils.recursive_bytetransform(res, decoder)


def infoget_decode(res):
    return utils.list2dict(res)


# These functions are only doing decoding on the output from redis
# decoder = staticmethod(decoder)
# decoding_functions = (
#     "loadbackend",
#     "modelstore",
#     "modelset",
#     "modeldel",
#     "modelexecute",
#     "modelrun",
#     "tensorset",
#     "scriptset",
#     "scriptstore",
#     "scriptdel",
#     "scriptrun",
#     "scriptexecute",
#     "inforeset",
# )
# for fn in decoding_functions:
#     setattr(Processor, fn, decoder)
