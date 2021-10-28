from .utils import list_to_dict
from ..helpers import nativestr


class TSInfo(object):
    """
    Hold information and statistics on the time-series.
    Can be created using ``tsinfo`` command
    https://oss.redis.com/redistimeseries/commands/#tsinfo.
    """

    rules = []
    labels = []
    sourceKey = None
    chunk_count = None
    memory_usage = None
    total_samples = None
    retention_msecs = None
    last_time_stamp = None
    first_time_stamp = None

    max_samples_per_chunk = None
    chunk_size = None
    duplicate_policy = None

    def __init__(self, args):
        """
        Hold information and statistics on the time-series.

        The supported params that can be passed as args:

        rules:
            A list of compaction rules of the time series.
        sourceKey:
            Key name for source time series in case the current series
            is a target of a rule.
        chunkCount:
            Number of Memory Chunks used for the time series.
        memoryUsage:
            Total number of bytes allocated for the time series.
        totalSamples:
            Total number of samples in the time series.
        labels:
            A list of label-value pairs that represent the metadata
            labels of the time series.
        retentionTime:
            Retention time, in milliseconds, for the time series.
        lastTimestamp:
            Last timestamp present in the time series.
        firstTimestamp:
            First timestamp present in the time series.
        maxSamplesPerChunk:
            Deprecated.
        chunkSize:
            Amount of memory, in bytes, allocated for data.
        duplicatePolicy:
            Policy that will define handling of duplicate samples.

        Can read more about on
        https://oss.redis.com/redistimeseries/configuration/#duplicate_policy
        """
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.rules = response["rules"]
        self.source_key = response["sourceKey"]
        self.chunk_count = response["chunkCount"]
        self.memory_usage = response["memoryUsage"]
        self.total_samples = response["totalSamples"]
        self.labels = list_to_dict(response["labels"])
        self.retention_msecs = response["retentionTime"]
        self.lastTimeStamp = response["lastTimestamp"]
        self.first_time_stamp = response["firstTimestamp"]
        if "maxSamplesPerChunk" in response:
            self.max_samples_per_chunk = response["maxSamplesPerChunk"]
            self.chunk_size = (
                self.max_samples_per_chunk * 16
            )  # backward compatible changes
        if "chunkSize" in response:
            self.chunk_size = response["chunkSize"]
        if "duplicatePolicy" in response:
            self.duplicate_policy = response["duplicatePolicy"]
            if type(self.duplicate_policy) == bytes:
                self.duplicate_policy = self.duplicate_policy.decode()
