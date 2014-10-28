from redis.connection import Token

cdef bytes SYM_STAR = b'*'
cdef bytes SYM_DOLLAR = b'$'
cdef bytes SYM_CRLF = b'\r\n'
cdef bytes SYM_LF = b'\n'

cdef extern from "Python.h":
    object PyObject_Str(object v)

cdef bytes _encode(self, value):
    "Return a bytestring representation of the value"
    if isinstance(value, bytes):
        return value
    elif isinstance(value, float):
        return bytes(repr(value), 'latin-1')
    elif not isinstance(value, basestring):
        value = PyObject_Str(value)
    elif isinstance(value, Token):
        return bytes(value.value, 'latin-1')

    if isinstance(value, unicode):
        value = (<unicode>value).encode(self.encoding, self.encoding_errors)
    return value

def _pack_command(self, *args):
    "Pack a series of arguments into a value Redis command"
    cdef bytes enc_value

    command = args[0]
    if ' ' in command:
        args = tuple([Token(s) for s in command.split(' ')]) + args[1:]
    else:
        args = (Token(command),) + args[1:]

    output = [SYM_STAR, bytes(PyObject_Str(len(args)), 'latin-1'), SYM_CRLF]
    for value in args:
        enc_value = _encode(self, value)
        output.append(SYM_DOLLAR)
        output.append(bytes(PyObject_Str(len(enc_value)), 'latin-1'))
        output.append(SYM_CRLF)
        output.append(enc_value)
        output.append(SYM_CRLF)
    return output
