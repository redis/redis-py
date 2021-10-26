from redis.client import bool_ok

from .utils import (
    parse_range,
    parse_get,
    parse_m_range,
    parse_m_get,
)
from .info import TSInfo
from ..helpers import parse_to_list
from .commands import (
    ALTER_CMD,
    CREATE_CMD,
    CREATERULE_CMD,
    DELETERULE_CMD,
    DEL_CMD,
    GET_CMD,
    INFO_CMD,
    MGET_CMD,
    MRANGE_CMD,
    MREVRANGE_CMD,
    QUERYINDEX_CMD,
    RANGE_CMD,
    REVRANGE_CMD,
    TimeSeriesCommands,
)


class TimeSeries(TimeSeriesCommands):
    """
    This class subclasses redis-py's `Redis` and implements RedisTimeSeries's
    commands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of it's
    functionality.
    """

    def __init__(self, client=None, version=None, **kwargs):
        """Create a new RedisTimeSeries client."""
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            CREATE_CMD: bool_ok,
            ALTER_CMD: bool_ok,
            CREATERULE_CMD: bool_ok,
            DEL_CMD: int,
            DELETERULE_CMD: bool_ok,
            RANGE_CMD: parse_range,
            REVRANGE_CMD: parse_range,
            MRANGE_CMD: parse_m_range,
            MREVRANGE_CMD: parse_m_range,
            GET_CMD: parse_get,
            MGET_CMD: parse_m_get,
            INFO_CMD: TSInfo,
            QUERYINDEX_CMD: parse_to_list,
        }

        self.client = client
        self.execute_command = client.execute_command
        self.MODULE_VERSION = version

        for k in MODULE_CALLBACKS:
            self.client.set_response_callback(k, MODULE_CALLBACKS[k])
