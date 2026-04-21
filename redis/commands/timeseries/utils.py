def _pairs_to_dict(pairs):
    """Convert a list of [key, value] pairs to a dict without forcing str."""
    return {pairs[i][0]: pairs[i][1] for i in range(len(pairs))}


def parse_range(response, **kwargs):
    """Parse range response. Used by TS.RANGE and TS.REVRANGE."""
    if not response:
        return []
    # Multi-aggregator: samples have >2 elements [timestamp, val1, val2, ...]
    if len(response[0]) > 2:
        return [[r[0]] + [float(v) for v in r[1:]] for r in response]
    return [[r[0], float(r[1])] for r in response]


def parse_m_range(response):
    """Parse multi range response. Used by TS.MRANGE and TS.MREVRANGE.

    Returns a dict keyed by time series name, matching RESP3 native format.
    Each value is [labels_dict, metadata, samples] where metadata is an empty
    list for RESP2 (RESP2 does not include the reducers/aggregators metadata
    that RESP3 returns as the second element).
    """
    res = {}
    for item in response:
        res[item[0]] = [_pairs_to_dict(item[1]), [], parse_range(item[2])]
    return res


def parse_get(response):
    """Parse get response. Used by TS.GET."""
    if not response:
        return None
    return [int(response[0]), float(response[1])]


def parse_m_get(response):
    """Parse multi get response. Used by TS.MGET.

    Returns a dict keyed by time series name, matching RESP3 native format.
    Each value is [labels_dict, [timestamp, value]] or [labels_dict, []] when
    no sample exists.
    """
    res = {}
    for item in response:
        if not item[2]:
            res[item[0]] = [_pairs_to_dict(item[1]), []]
        else:
            res[item[0]] = [
                _pairs_to_dict(item[1]),
                [int(item[2][0]), float(item[2][1])],
            ]
    return res
