
cdef bytes SYM_STAR = b'*'
cdef bytes SYM_DOLLAR = b'$'
cdef bytes SYM_CRLF = b'\r\n'
cdef bytes SYM_LF = b'\n'

DEF CHAR_BIT = 8

cdef bytes size_to_decimal_bytes(Py_ssize_t n):
    # sizeof(long)*CHAR_BIT/3+6
    cdef char buf[32]
    cdef char *p
    cdef char *bufend
    cdef unsigned long absn
    cdef char c = '0'
    p = bufend = buf + sizeof(buf)
    if n < 0:
        absn = 0UL - n
    else:
        absn = n
    while True:
        p -= 1
        p[0] = c + (absn % 10)
        absn /= 10
        if absn == 0:
            break
    if n < 0:
        p -= 1
        p[0] = '-'
    return p[:(bufend-p)]

cdef bytes int_to_decimal_bytes(n):
    if isinstance(n, int):
        return size_to_decimal_bytes(n)
    elif isinstance(n, long):
        return bytes(str(n), 'latin-1')
    raise ValueError

cdef bytes _encode(self, value):
    "Return a bytestring representation of the value"
    if isinstance(value, bytes):
        return value
    elif isinstance(value, float):
        return bytes(repr(value), 'latin-1')
    elif isinstance(value, (int, long)):
        return int_to_decimal_bytes(value)
    elif not isinstance(value, basestring):
        value = str(value)

    if isinstance(value, unicode):
        value = (<unicode>value).encode(self.encoding, self.encoding_errors)
    return value

def _pack_command(self, *args):
    "Pack a series of arguments into a value Redis command"
    cdef int i
    cdef bytes enc_value, s

    args = tuple(args[0].split(' ')) + args[1:]

    cdef list chunks = []
    cdef list chunk = [SYM_STAR, size_to_decimal_bytes(len(args)), SYM_CRLF]
    cdef int chunk_size = 0
    for s in chunk:
        chunk_size += len(s)

    for value in args:
        enc_value = _encode(self, value)

        if chunk_size > 6000 or len(enc_value) > 6000:
            chunks.append(b''.join(chunk))
            chunk = []
            chunk_size = 0

        chunk.append(SYM_DOLLAR)
        chunk_size += len(SYM_DOLLAR)

        s = size_to_decimal_bytes(len(enc_value))
        chunk.append(s)
        chunk_size += len(s)

        chunk.append(SYM_CRLF)
        chunk_size += len(SYM_CRLF)

        chunk.append(enc_value)
        chunk_size += len(enc_value)

        chunk.append(SYM_CRLF)
        chunk_size += len(SYM_CRLF)

    if chunk:
        chunks.append(b''.join(chunk))
    return chunks
