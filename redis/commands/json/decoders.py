from ..helpers import delist


def decode_toggle(b):
    if isinstance(b, list):
        return b
    return b == b"true"

def decode_list_or_int(b):
    if isinstance(b, list):
        return b
    return int(b)