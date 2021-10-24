import six


def to_string(s):
    if isinstance(s, six.string_types):
        return s
    elif isinstance(s, six.binary_type):
        return s.decode("utf-8", "ignore")
    else:
        return s  # Not a string we care about
