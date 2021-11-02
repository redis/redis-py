def decode_list_or_int(b):
    if isinstance(b, list):
        return b
    if b is None:
        return None
    elif b == b"true":
        return True
    elif b == b"false":
        return False
    return int(b)
