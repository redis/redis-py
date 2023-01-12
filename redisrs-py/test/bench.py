from random import choice
from string import ascii_uppercase
import timeit
import redisrs_py
import hiredis

###############################################################
#    COPY-PASTE
###############################################################

encoding="utf-8"
encoding_errors="strict"
decode_responses=False

#pure copy-paste from connetion.py

class Encoder:
    "Encode strings to bytes-like and decode bytes-like to strings"

    def __init__(self, encoding, encoding_errors, decode_responses):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    def encode(self, value):
        "Return a bytestring or bytes-like representation of the value"
        if isinstance(value, (bytes, memoryview)):
            return value
        elif isinstance(value, bool):
            # special case bool since it is a subclass of int
            raise Exception(
                "Invalid input of type: 'bool'. Convert to a "
                "bytes, string, int or float first."
            )
        elif isinstance(value, (int, float)):
            value = repr(value).encode()
        elif not isinstance(value, str):
            # a value we don't know how to deal with. throw an error
            typename = type(value).__name__
            raise Exception(
                f"Invalid input of type: '{typename}'. {value}"
                f"Convert to a bytes, string, int or float first."
            )
        if isinstance(value, str):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def decode(self, value, force=False):
        "Return a unicode string from the bytes-like representation"
        if self.decode_responses or force:
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                value = value.decode(self.encoding, self.encoding_errors)
        return value

SYM_STAR = b'*'
SYM_DOLLAR = b'$'
SYM_CRLF = b'\r\n'
SYM_EMPTY = b''

encoder = Encoder(encoding, encoding_errors, decode_responses)

#almost pure copy-paste from connetion.py: got rid of self.encoder and self.buffer_cutoff

def pack_command(*args):
    """Pack a series of arguments into the Redis protocol"""
    output = []
    # the client might have included 1 or more literal arguments in
    # the command name, e.g., 'CONFIG GET'. The Redis server expects these
    # arguments to be sent separately, so split the first argument
    # manually. These arguments should be bytestrings so that they are
    # not encoded.
    if isinstance(args[0], str):
        args = tuple(args[0].encode().split()) + args[1:]
    elif b" " in args[0]:
        args = tuple(args[0].split()) + args[1:]
    buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

    buffer_cutoff = 6000
    global encoder
    for arg in map(encoder.encode, args):
        # to avoid large string mallocs, chunk the command into the
        # output list if we're sending large values or memoryviews
        arg_length = len(arg)
        if (
            len(buff) > buffer_cutoff
            or arg_length > buffer_cutoff
            or isinstance(arg, memoryview)
        ):
            buff = SYM_EMPTY.join(
                (buff, SYM_DOLLAR, str(arg_length).encode(), SYM_CRLF)
            )
            output.append(buff)
            output.append(arg)
            buff = SYM_CRLF
        else:
            buff = SYM_EMPTY.join(
                (
                    buff,
                    SYM_DOLLAR,
                    str(arg_length).encode(),
                    SYM_CRLF,
                    arg,
                    SYM_CRLF,
                )
            )
    output.append(buff)
    return output


###############################################################
#    WRAPPER FUNCTIONS
#    pack_command_* - encoding on module extention side
#    pack_bytes_* encoding on python side
###############################################################


def pack_bytes_hiredis(*args):
    """Pack a series of arguments into the Redis protocol"""
    output = []
    if isinstance(args[0], str):
        args = tuple(args[0].encode().split()) + args[1:]
    elif b" " in args[0]:
        args = tuple(args[0].split()) + args[1:]

    global encoder
    cmd = b' '.join(map(encoder.encode, args))

    output.append(hiredis.pack_bytes(cmd))
    return output


def pack_command_hiredis(*args):
    """Pack a series of arguments into the Redis protocol"""
    output = []
    if isinstance(args[0], str):
        args = tuple(args[0].encode().split()) + args[1:]
    elif b" " in args[0]:
        args = tuple(args[0].split()) + args[1:]
    output.append(hiredis.pack_command(args))
    return output

#Rust
def pack_bytes_rust(*args):
    """Pack a series of arguments into the Redis protocol"""
    output = []
    if isinstance(args[0], str):
        args = tuple(args[0].encode().split()) + args[1:]
    elif b" " in args[0]:
        args = tuple(args[0].split()) + args[1:]

    global encoder
    cmd = b' '.join(map(encoder.encode, args))

    output.append(redisrs_py.pack_bytes(cmd))
    return output


def pack_command_rust(*args):
    """Pack a series of arguments into the Redis protocol"""
    output = []
    if isinstance(args[0], str):
        args = tuple(args[0].encode().split()) + args[1:]
    elif b" " in args[0]:
        args = tuple(args[0].split()) + args[1:]
    output.append(redisrs_py.pack_command(args))
    return output


###############################################################
#    BENCHMARKING
###############################################################


TOTAL_CMDS = 100000

def get_entry(number_of_fields):
    fields = (b'HSET', f'foo42')
    for i in range(number_of_fields):
        fields += (f"field{i}",)
        fields += (''.join(choice(ascii_uppercase) for _ in range(10)),)
    return fields


def get_input(number_of_fields, total=TOTAL_CMDS):
    entry = get_entry(number_of_fields)
    input = [] 
    for _ in range(total):
        input.append(entry)
    return input


input_0 = []
for i in range(TOTAL_CMDS):
    input_0.append(
        (b'HSET', f'foo{i}', 42, 'zoo', 'a', 67, 43, 3.56, 'foo1', 'qwertyuiop', 'foo2', 3.1412345678901, 52, 'zxcvbnmmpei', 53, 'dhjihoihpouihj', 'kjl;kjhj;lkjlk;', '567890798783', 'kjkjkjkjk', 79878933334890808709890.0)
    )
input_17 = get_input(17)
input_50 = get_input(50)
input_100 = get_input(100)


ITERATIONS = 5


def bench(input_name, description):
    print(description)
    res = timeit.timeit(f'[pack_command(*cmd) for cmd in {input_name}]', globals=globals(), number=ITERATIONS)
    print(f'BASELINE:redis.py: {res}')

    print("IMPLEMENTATIONS:")
    res = timeit.timeit(f'[pack_bytes_hiredis(*cmd) for cmd in {input_name}]', globals=globals(), number=ITERATIONS)
    print(f'hiredis.pack_bytes: {res}')

    res = timeit.timeit(f'[pack_bytes_rust(*cmd) for cmd in {input_name}]', globals=globals(), number=ITERATIONS)
    print(f'redisrs-py.pack_bytes: {res}')

    if input_name == 'input_0':
        print('hiredis.pack_command - not supported (yet?!)')
    else:
        res = timeit.timeit(f'[pack_command_hiredis(*cmd) for cmd in {input_name}]', globals=globals(), number=ITERATIONS)
        print(f'hiredis.pack_command(partial encoding in hiredis-C): {res}')

    res = timeit.timeit(f'[pack_command_rust(*cmd) for cmd in {input_name}]', globals=globals(), number=ITERATIONS)
    print(f'redisrs_py.pack_command(encoding in Rust): {res}')

bench("input_0", "========= 10 fields of various types: str, int, float, bytes")
bench("input_17", "\n========= 17 10-bytes fileds per entry")
bench("input_50", "\n========= 50 10-bytes fileds per entry")
bench("input_100", "\n========= 100 10-bytes fileds per entry")
