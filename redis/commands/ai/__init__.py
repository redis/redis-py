from functools import wraps

from .commands import AICommands
from .pipeline import Pipeline
from .postprocessor import *  # noqa


class AI(AICommands):
    """
    Redis client build specifically for the RedisAI module. It takes all the necessary
    parameters to establish the connection and an optional ``debug`` parameter on
    initialization

    Parameters
    ----------

    debug : bool
        If debug mode is ON, then each command that is sent to the server is
        printed to the terminal
    enable_postprocess : bool
        Flag to enable post processing. If enabled, all the bytestring-ed returns
        are converted to python strings recursively and key value pairs will be
        converted to dictionaries. Also note that, this flag doesn't work with
        pipeline() function since pipeline function could have native redis commands
        (along with RedisAI commands)

    Example
    -------
    >>> from redisai import Client
    >>> con = Client(host='localhost', port=6379)
    """

    def __init__(self, client, debug=False, enable_postprocess=True):
        self.client = client
        self.enable_postprocess = enable_postprocess

        if debug:
            self.execute_command = enable_debug(self.client.execute_command)
        else:
            self.execute_command = client.execute_command

        MODULE_CALLBACKS = {
            # "AI.LOADBACKEND": decoder,
            "AI.MODELGET": decode_modelget,
            # "AI.MODELSET": decoder,
            "AI.MODELSCAN": decode_modelscan,
            # "AI.MODELSTORE": decoder,
            # "AI.MODELDEL": decoder,
            # "AI.MODELRUN": decoder,
            # "AI.MODELEXECUTE": decoder,
            "AI.TENSORGET": decode_tensorget,
            # "AI.TENSORSET": decoder,
            "AI.SCRIPTGET": decode_scriptget,
            "AI._SCRIPTSCAN": decode_scriptscan,
            # "AI.SCRIPTSET": decoder,
            # "AI.SCRIPTSTORE": decoder,
            # "AI.SCRIPTDEL": decoder,
            # "AI.SCRIPTRUN": decoder,
            # "AI.SCRIPTEXECUTE": decoder,
            # "AI.INFO": decode_infoget,
        }
        for k, v in MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)

    def pipeline(self, transaction: bool = True, shard_hint: bool = None) -> "Pipeline":
        """
        It follows the same pipeline implementation of native redis
        client but enables it to access redisai operation as well.
        This function is experimental in the current release

        Example
        -------
        >>> pipe = con.pipeline(transaction=False)
        >>> pipe = pipe.set('nativeKey', 1)
        >>> pipe = pipe.tensorset('redisaiKey', np.array([1, 2]))
        >>> pipe.execute()
        [True, b'OK']
        """
        return Pipeline(
            self.enable_postprocess,
            self.connection_pool,
            self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )


def enable_debug(f):
    @wraps(f)
    def wrapper(*args):
        print(*args)
        return f(*args)

    return wrapper
