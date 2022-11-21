def parse_tdigest_quantile(response):
    """Parse TDIGEST.QUANTILE response."""
    return [float(x) for x in response]
