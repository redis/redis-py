from ..helpers import nativestr


class BFInfo(object):
    capacity = None
    size = None
    filterNum = None
    insertedNum = None
    expansionRate = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.capacity = response["Capacity"]
        self.size = response["Size"]
        self.filterNum = response["Number of filters"]
        self.insertedNum = response["Number of items inserted"]
        self.expansionRate = response["Expansion rate"]


class CFInfo(object):
    size = None
    bucketNum = None
    filterNum = None
    insertedNum = None
    deletedNum = None
    bucketSize = None
    expansionRate = None
    maxIteration = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.size = response["Size"]
        self.bucketNum = response["Number of buckets"]
        self.filterNum = response["Number of filters"]
        self.insertedNum = response["Number of items inserted"]
        self.deletedNum = response["Number of items deleted"]
        self.bucketSize = response["Bucket size"]
        self.expansionRate = response["Expansion rate"]
        self.maxIteration = response["Max iterations"]


class CMSInfo(object):
    width = None
    depth = None
    count = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.width = response["width"]
        self.depth = response["depth"]
        self.count = response["count"]


class TopKInfo(object):
    k = None
    width = None
    depth = None
    decay = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.k = response["k"]
        self.width = response["width"]
        self.depth = response["depth"]
        self.decay = response["decay"]


class TDigestInfo(object):
    compression = None
    capacity = None
    mergedNodes = None
    unmergedNodes = None
    mergedWeight = None
    unmergedWeight = None
    totalCompressions = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.compression = response["Compression"]
        self.capacity = response["Capacity"]
        self.mergedNodes = response["Merged nodes"]
        self.unmergedNodes = response["Unmerged nodes"]
        self.mergedWeight = response["Merged weight"]
        self.unmergedWeight = response["Unmerged weight"]
        self.totalCompressions = response["Total compressions"]
