#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* encode speedup */
typedef struct {
    PyObject_HEAD;
    const char *encoding;
    const char *errors;
} EncoderObject;

static PyObject *Encoder_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    EncoderObject *self = (EncoderObject *)type->tp_alloc(type, 0);
    if (!self) {
        return NULL;
    }

    self->encoding = NULL;
    self->errors = "strict";
    return (PyObject *)self;
}

static int Encoder_init(EncoderObject *self, PyObject *args, PyObject *kwargs) {
    if (!PyArg_ParseTuple(args, "ss", &self->encoding, &self->errors)) {
        return -1;
    }

    if (!strcmp(self->encoding, "utf-8") && !strcmp(self->errors, "strict")) {
        self->encoding = NULL;
    }
    return 0;
}

static void Encoder_dealloc(EncoderObject *self) {
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Encoder___getstate__(EncoderObject *self) {
    return Py_BuildValue(
        "(ss)",
        self->encoding ? self->encoding : "utf-8",
        self->errors
    );
}

static PyObject *Encoder___setstate__(EncoderObject *self, PyObject *state) {
    if (!PyArg_ParseTuple(state, "ss", &self->encoding, &self->errors)) {
        return NULL;
    }

    if (!strcmp(self->encoding, "utf-8") && !strcmp(self->errors, "strict")) {
        self->encoding = NULL;
    }
    Py_RETURN_NONE;
}

static inline PyObject *encode(
    PyObject *arg,
    char **item,
    Py_ssize_t *item_len,
    const char *encoding,
    const char *errors
) {
    PyObject *tmp;
    if (PyUnicode_Check(arg)) {
        if (encoding) {
            tmp = PyUnicode_AsEncodedString(arg, encoding, errors);
            *item = PyBytes_AS_STRING(tmp);
            *item_len = PyBytes_GET_SIZE(tmp);
            return tmp;
        } else {
            *item = (char *)PyUnicode_AsUTF8AndSize(arg, item_len);
            return NULL;
        }
    }
    if (PyBytes_Check(arg)) {
        *item = PyBytes_AS_STRING(arg);
        *item_len = PyBytes_GET_SIZE(arg);
        return NULL;
    }
    if (PyLong_Check(arg) || PyFloat_Check(arg)) {
        if (!PyBool_Check(arg)) {
            tmp = PyObject_Repr(arg);
            *item = (char *)PyUnicode_AsUTF8AndSize(tmp, item_len);
            return tmp;
        }
    }
    *item = NULL;
    if (PyMemoryView_Check(arg)) {
        *item_len = PyMemoryView_GET_BUFFER(arg)->len;
        return NULL;
    }
    *item_len = 0;
    return NULL;
}
/* !encode speedup */

/* key_slot speedup */

/* CRC16 implementation according to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */
static const uint16_t crc16tab[256]= {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

/*
 * See: https://redis.io/docs/reference/cluster-spec/#appendix-a-crc16-reference-implementation-in-ansi-c
 */
static inline uint16_t crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

/*
 * See: https://redis.io/docs/reference/cluster-spec/#hash-tags
 */
static inline unsigned int HASH_SLOT(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    /* Search the first occurrence of '{'. */
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 16383;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 16383;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 16383;
}

static PyObject *KeySlotter_key_slot(EncoderObject *self, PyObject *key) {
    char *item;
    Py_ssize_t item_len;
    PyObject *tmp = encode(key, &item, &item_len, self->encoding, self->errors);
    if (!item) {
        return NULL;
    }

    unsigned int hash_slot = HASH_SLOT(item, item_len);
    if (tmp) {
        Py_CLEAR(tmp);
    }
    return PyLong_FromUnsignedLong(hash_slot);
}
/* !key_slot speedup */

/* pack_command(s) speedup */
static const char *const SYM_STAR = "*";
static const char *const SYM_DOLLAR = "$";
static const char *const SYM_CRLF = "\r\n";

#define ENSURE_SIZE(len)                                                       \
    if (*buf_len < len) {                                                      \
        while (*buf_len < len)                                                 \
            *buf_len = *buf_len * 2;                                           \
        *buf = (char *)PyMem_RawRealloc(*buf, *buf_len);                       \
        if (!*buf)                                                             \
            return;                                                            \
    }

#define ENSURE_BUF                                                             \
    if (!*buf) {                                                               \
        Py_CLEAR(ret);                                                         \
        Py_CLEAR(cmd);                                                         \
        Py_CLEAR(tmp);                                                         \
        return PyErr_NoMemory();                                               \
    }

static inline void cpy_char_int_crlf(
    char **buf,
    size_t *buf_pos,
    size_t *buf_len,
    const char *const _char,
    Py_ssize_t _int,
    char buf_tmp[]
) {
    size_t len = sprintf(buf_tmp, "%zd", _int);
    ENSURE_SIZE(*buf_pos + len + 3);

    memcpy(*buf  +  *buf_pos,               _char,          1);
    memcpy(*buf  +  *buf_pos + 1,           buf_tmp,        len);
    memcpy(*buf  +  *buf_pos + 1 + len,     SYM_CRLF,       2);
    *buf_pos += len + 3;
}

static inline void cpy_str_crlf(
    char **buf,
    size_t *buf_pos,
    size_t *buf_len,
    char *_str,
    Py_ssize_t len
) {
    ENSURE_SIZE(*buf_pos + len + 2);

    memcpy(*buf  +  *buf_pos,               _str,           len);
    memcpy(*buf  +  *buf_pos + len,         SYM_CRLF,       2);
    *buf_pos += len + 2;
}

static inline void PyList_Append_str(
    PyObject *list,
    char *item,
    Py_ssize_t item_len
) {
    PyObject *tmp = PyBytes_FromStringAndSize(item, item_len);
    PyList_Append(list, tmp);
    Py_DECREF(tmp);
}

static inline void PyList_Append_buf(
    PyObject *list,
    char *buf,
    size_t *buf_pos
) {
    PyList_Append_str(list, buf, *buf_pos);

    memcpy(buf,                             SYM_CRLF,       2);
    *buf_pos = 2;
}

static inline PyObject *pack_command(
    PyObject *ret,
    PyObject *command,
    char **buf,
    size_t *buf_pos,
    size_t *buf_len,
    char buf_tmp[],
    const char *encoding,
    const char *errors
) {
    PyObject *cmd = PyTuple_GET_ITEM(command, 0);
    PyObject *arg;
    PyObject *tmp;
    Py_ssize_t command_len = PyTuple_GET_SIZE(command);
    Py_ssize_t cmd_len;
    Py_ssize_t item_len;
    char *item;

    if (PyBytes_Check(cmd)) {
        tmp = PyUnicode_FromString(PyBytes_AS_STRING(cmd));
        cmd = PyUnicode_Split(tmp, NULL, -1);
        Py_CLEAR(tmp);
    } else if (PyUnicode_Check(cmd)) {
        cmd = PyUnicode_Split(cmd, NULL, -1);
    } else {
        Py_CLEAR(ret);
        PyMem_RawFree(*buf);
        *buf = NULL;
        return NULL;
    }
    cmd_len = PyList_GET_SIZE(cmd);

    cpy_char_int_crlf(
        buf,
        buf_pos,
        buf_len,
        SYM_STAR,
        command_len + cmd_len - 1,
        buf_tmp
    );
    for (int i = 0; i < command_len + cmd_len - 1; i = i + 1) {
        if (i < cmd_len) {
            arg = PyList_GET_ITEM(cmd, i);
            tmp = encode(arg, &item, &item_len, NULL, NULL);
        } else {
            arg = PyTuple_GET_ITEM(command, i - cmd_len + 1);
            tmp = encode(arg, &item, &item_len, encoding, errors);
        }

        if (item) {
            cpy_char_int_crlf(
                buf,
                buf_pos,
                buf_len,
                SYM_DOLLAR,
                item_len,
                buf_tmp
            );
            ENSURE_BUF;

            if (item_len < 4096) {
                cpy_str_crlf(
                    buf,
                    buf_pos,
                    buf_len,
                    item,
                    item_len
                );
                ENSURE_BUF;
            } else {
                PyList_Append_buf(ret, *buf, buf_pos);
                if (tmp && PyBytes_Check(tmp)) {
                    PyList_Append(ret, tmp);
                } else if (PyBytes_Check(arg)) {
                    PyList_Append(ret, arg);
                } else {
                    PyList_Append_str(ret, item, item_len);
                }
            }
        } else {
            if (item_len) {
                cpy_char_int_crlf(
                    buf,
                    buf_pos,
                    buf_len,
                    SYM_DOLLAR,
                    item_len,
                    buf_tmp
                );
                ENSURE_BUF;

                PyList_Append_buf(ret, *buf, buf_pos);
                PyList_Append(ret, arg);
            } else {
                Py_CLEAR(tmp);
                Py_CLEAR(cmd);
                Py_CLEAR(ret);
                PyMem_RawFree(*buf);
                *buf = NULL;
                return NULL;
            }
        }
        if (tmp) {
            Py_CLEAR(tmp);
        }
    }
    Py_CLEAR(cmd);

    return ret;
}

static PyObject *CommandPacker_pack_command(EncoderObject *self, PyObject *command) {
    size_t buf_len = 4096;
    char *buf = (char *)PyMem_RawMalloc(buf_len);
    if (!buf) {
        return PyErr_NoMemory();
    }
    size_t buf_pos = 0;
    char buf_tmp[16];

    PyObject *ret = PyList_New(0);

    ret = pack_command(
        ret,
        command,
        &buf,
        &buf_pos,
        &buf_len,
        buf_tmp,
        self->encoding,
        self->errors
    );
    if (buf) {
        PyList_Append_buf(ret, buf, &buf_pos);
        PyMem_RawFree(buf);
    }

    return ret;
}

static PyObject *CommandPacker_pack_commands(EncoderObject *self, PyObject *commands) {
    size_t buf_len = 4096;
    char *buf = (char *)PyMem_RawMalloc(buf_len);
    if (!buf) {
        return PyErr_NoMemory();
    }
    size_t buf_pos = 0;
    char buf_tmp[16];

    PyObject *ret = PyList_New(0);
    PyObject *command;

    while ((command = PyIter_Next(commands))) {
        ret = pack_command(
            ret,
            command,
            &buf,
            &buf_pos,
            &buf_len,
            buf_tmp,
            self->encoding,
            self->errors
        );
        Py_DECREF(command);
        if (!buf) {
            return ret;
        }
    }
    if (buf) {
        PyList_Append_buf(ret, buf, &buf_pos);
        PyMem_RawFree(buf);
    }

    return ret;
}
/* !pack_command(s) speedup */

/* speedup module */
static PyMethodDef speedups_KeySlotterMethods[] = {
    {"__getstate__", (PyCFunction)Encoder___getstate__, METH_NOARGS, NULL},
    {"__setstate__", (PyCFunction)Encoder___setstate__, METH_O, NULL},
    {"key_slot", (PyCFunction)KeySlotter_key_slot, METH_O, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject speedups_KeySlotterType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "speedups.KeySlotter",
    .tp_methods = speedups_KeySlotterMethods,
    .tp_basicsize = sizeof(EncoderObject),
    .tp_new = (newfunc)Encoder_new,
    .tp_init = (initproc)Encoder_init,
    .tp_dealloc = (destructor)Encoder_dealloc,
};

static PyMethodDef speedups_CommandPackerMethods[] = {
    {"__getstate__", (PyCFunction)Encoder___getstate__, METH_NOARGS, NULL},
    {"__setstate__", (PyCFunction)Encoder___setstate__, METH_O, NULL},
    {"pack_command", (PyCFunction)CommandPacker_pack_command, METH_O, NULL},
    {"pack_commands", (PyCFunction)CommandPacker_pack_commands, METH_O, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject speedups_CommandPackerType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "speedups.CommandPacker",
    .tp_methods = speedups_CommandPackerMethods,
    .tp_basicsize = sizeof(EncoderObject),
    .tp_new = (newfunc)Encoder_new,
    .tp_init = (initproc)Encoder_init,
    .tp_dealloc = (destructor)Encoder_dealloc,
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "speedups",
    .m_doc = PyDoc_STR("Speedups for redis-py"),
    .m_size = 0,
};

PyMODINIT_FUNC
PyInit_speedups(void) {
    if (PyType_Ready(&speedups_KeySlotterType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&speedups_CommandPackerType) < 0) {
        return NULL;
    }

    PyObject *m = PyModule_Create(&module);
    if (!m) {
        return NULL;
    }

    Py_INCREF(&speedups_KeySlotterType);
    if (PyModule_AddObject(m, "KeySlotter", (PyObject *)&speedups_KeySlotterType) < 0) {
        Py_DECREF(&speedups_KeySlotterType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&speedups_CommandPackerType);
    if (PyModule_AddObject(m, "CommandPacker", (PyObject *)&speedups_CommandPackerType) < 0) {
        Py_DECREF(&speedups_CommandPackerType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
/* !speedup module */
