def int_or_list(b):
    if isinstance(b, int):
        return b
    else:
        return b


def int_or_none(b):
    if b is None:
        return None
    if isinstance(b, int):
        return b
