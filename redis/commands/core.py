import datetime
import time
import warnings
import hashlib

from .helpers import list_or_args
from redis.exceptions import (
    ConnectionError,
    DataError,
    NoScriptError,
    RedisError,
)


class CoreCommands:
    """
    A class containing all of the implemented redis commands. This class is
    to be used as a mixin.
    """

    # SERVER INFORMATION

    # ACL methods
    def acl_cat(self, category=None):
        """
        Returns a list of categories or commands within a category.

        If ``category`` is not supplied, returns a list of all categories.
        If ``category`` is supplied, returns a list of all commands within
        that category.

        For more information check https://redis.io/commands/acl-cat
        """
        pieces = [category] if category else []
        return self.execute_command('ACL CAT', *pieces)

    def acl_deluser(self, *username):
        """
        Delete the ACL for the specified ``username``s

        For more information check https://redis.io/commands/acl-deluser
        """
        return self.execute_command('ACL DELUSER', *username)

    def acl_genpass(self, bits=None):
        """Generate a random password value.
        If ``bits`` is supplied then use this number of bits, rounded to
        the next multiple of 4.
        See: https://redis.io/commands/acl-genpass
        """
        pieces = []
        if bits is not None:
            try:
                b = int(bits)
                if b < 0 or b > 4096:
                    raise ValueError
            except ValueError:
                raise DataError('genpass optionally accepts a bits argument, '
                                'between 0 and 4096.')
        return self.execute_command('ACL GENPASS', *pieces)

    def acl_getuser(self, username):
        """
        Get the ACL details for the specified ``username``.

        If ``username`` does not exist, return None

        For more information check https://redis.io/commands/acl-getuser
        """
        return self.execute_command('ACL GETUSER', username)

    def acl_help(self):
        """The ACL HELP command returns helpful text describing
        the different subcommands.

        For more information check https://redis.io/commands/acl-help
        """
        return self.execute_command('ACL HELP')

    def acl_list(self):
        """
        Return a list of all ACLs on the server

        For more information check https://redis.io/commands/acl-list
        """
        return self.execute_command('ACL LIST')

    def acl_log(self, count=None):
        """
        Get ACL logs as a list.
        :param int count: Get logs[0:count].
        :rtype: List.

        For more information check https://redis.io/commands/acl-log
        """
        args = []
        if count is not None:
            if not isinstance(count, int):
                raise DataError('ACL LOG count must be an '
                                'integer')
            args.append(count)

        return self.execute_command('ACL LOG', *args)

    def acl_log_reset(self):
        """
        Reset ACL logs.
        :rtype: Boolean.

        For more information check https://redis.io/commands/acl-log
        """
        args = [b'RESET']
        return self.execute_command('ACL LOG', *args)

    def acl_load(self):
        """
        Load ACL rules from the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to load ACL rules from an aclfile.

        For more information check https://redis.io/commands/acl-load
        """
        return self.execute_command('ACL LOAD')

    def acl_save(self):
        """
        Save ACL rules to the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to save ACL rules to an aclfile.

        For more information check https://redis.io/commands/acl-save
        """
        return self.execute_command('ACL SAVE')

    def acl_setuser(self, username, enabled=False, nopass=False,
                    passwords=None, hashed_passwords=None, categories=None,
                    commands=None, keys=None, reset=False, reset_keys=False,
                    reset_passwords=False):
        """
        Create or update an ACL user.

        Create or update the ACL for ``username``. If the user already exists,
        the existing ACL is completely overwritten and replaced with the
        specified values.

        ``enabled`` is a boolean indicating whether the user should be allowed
        to authenticate or not. Defaults to ``False``.

        ``nopass`` is a boolean indicating whether the can authenticate without
        a password. This cannot be True if ``passwords`` are also specified.

        ``passwords`` if specified is a list of plain text passwords
        to add to or remove from the user. Each password must be prefixed with
        a '+' to add or a '-' to remove. For convenience, the value of
        ``passwords`` can be a simple prefixed string when adding or
        removing a single password.

        ``hashed_passwords`` if specified is a list of SHA-256 hashed passwords
        to add to or remove from the user. Each hashed password must be
        prefixed with a '+' to add or a '-' to remove. For convenience,
        the value of ``hashed_passwords`` can be a simple prefixed string when
        adding or removing a single password.

        ``categories`` if specified is a list of strings representing category
        permissions. Each string must be prefixed with either a '+' to add the
        category permission or a '-' to remove the category permission.

        ``commands`` if specified is a list of strings representing command
        permissions. Each string must be prefixed with either a '+' to add the
        command permission or a '-' to remove the command permission.

        ``keys`` if specified is a list of key patterns to grant the user
        access to. Keys patterns allow '*' to support wildcard matching. For
        example, '*' grants access to all keys while 'cache:*' grants access
        to all keys that are prefixed with 'cache:'. ``keys`` should not be
        prefixed with a '~'.

        ``reset`` is a boolean indicating whether the user should be fully
        reset prior to applying the new ACL. Setting this to True will
        remove all existing passwords, flags and privileges from the user and
        then apply the specified rules. If this is False, the user's existing
        passwords, flags and privileges will be kept and any new specified
        rules will be applied on top.

        ``reset_keys`` is a boolean indicating whether the user's key
        permissions should be reset prior to applying any new key permissions
        specified in ``keys``. If this is False, the user's existing
        key permissions will be kept and any new specified key permissions
        will be applied on top.

        ``reset_passwords`` is a boolean indicating whether to remove all
        existing passwords and the 'nopass' flag from the user prior to
        applying any new passwords specified in 'passwords' or
        'hashed_passwords'. If this is False, the user's existing passwords
        and 'nopass' status will be kept and any new specified passwords
        or hashed_passwords will be applied on top.

        For more information check https://redis.io/commands/acl-setuser
        """
        encoder = self.connection_pool.get_encoder()
        pieces = [username]

        if reset:
            pieces.append(b'reset')

        if reset_keys:
            pieces.append(b'resetkeys')

        if reset_passwords:
            pieces.append(b'resetpass')

        if enabled:
            pieces.append(b'on')
        else:
            pieces.append(b'off')

        if (passwords or hashed_passwords) and nopass:
            raise DataError('Cannot set \'nopass\' and supply '
                            '\'passwords\' or \'hashed_passwords\'')

        if passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            passwords = list_or_args(passwords, [])
            for i, password in enumerate(passwords):
                password = encoder.encode(password)
                if password.startswith(b'+'):
                    pieces.append(b'>%s' % password[1:])
                elif password.startswith(b'-'):
                    pieces.append(b'<%s' % password[1:])
                else:
                    raise DataError('Password %d must be prefixeed with a '
                                    '"+" to add or a "-" to remove' % i)

        if hashed_passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            hashed_passwords = list_or_args(hashed_passwords, [])
            for i, hashed_password in enumerate(hashed_passwords):
                hashed_password = encoder.encode(hashed_password)
                if hashed_password.startswith(b'+'):
                    pieces.append(b'#%s' % hashed_password[1:])
                elif hashed_password.startswith(b'-'):
                    pieces.append(b'!%s' % hashed_password[1:])
                else:
                    raise DataError('Hashed %d password must be prefixeed '
                                    'with a "+" to add or a "-" to remove' % i)

        if nopass:
            pieces.append(b'nopass')

        if categories:
            for category in categories:
                category = encoder.encode(category)
                # categories can be prefixed with one of (+@, +, -@, -)
                if category.startswith(b'+@'):
                    pieces.append(category)
                elif category.startswith(b'+'):
                    pieces.append(b'+@%s' % category[1:])
                elif category.startswith(b'-@'):
                    pieces.append(category)
                elif category.startswith(b'-'):
                    pieces.append(b'-@%s' % category[1:])
                else:
                    raise DataError('Category "%s" must be prefixed with '
                                    '"+" or "-"'
                                    % encoder.decode(category, force=True))
        if commands:
            for cmd in commands:
                cmd = encoder.encode(cmd)
                if not cmd.startswith(b'+') and not cmd.startswith(b'-'):
                    raise DataError('Command "%s" must be prefixed with '
                                    '"+" or "-"'
                                    % encoder.decode(cmd, force=True))
                pieces.append(cmd)

        if keys:
            for key in keys:
                key = encoder.encode(key)
                pieces.append(b'~%s' % key)

        return self.execute_command('ACL SETUSER', *pieces)

    def acl_users(self):
        """Returns a list of all registered users on the server.

        For more information check https://redis.io/commands/acl-users
        """
        return self.execute_command('ACL USERS')

    def acl_whoami(self):
        """Get the username for the current connection

        For more information check https://redis.io/commands/acl-whoami
        """
        return self.execute_command('ACL WHOAMI')

    def bgrewriteaof(self):
        """Tell the Redis server to rewrite the AOF file from data in memory.

        For more information check https://redis.io/commands/bgrewriteaof
        """
        return self.execute_command('BGREWRITEAOF')

    def bgsave(self, schedule=True):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.

        For more information check https://redis.io/commands/bgsave
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command('BGSAVE', *pieces)

    def client_kill(self, address):
        """Disconnects the client at ``address`` (ip:port)

        For more information check https://redis.io/commands/client-kill
        """
        return self.execute_command('CLIENT KILL', address)

    def client_kill_filter(self, _id=None, _type=None, addr=None,
                           skipme=None, laddr=None, user=None):
        """
        Disconnects client(s) using a variety of filter options
        :param id: Kills a client by its unique ID field
        :param type: Kills a client by type where type is one of 'normal',
        'master', 'slave' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        :param laddr: Kills a client by its 'local (bind) address:port'
        :param user: Kills a client for a specific user name
        """
        args = []
        if _type is not None:
            client_types = ('normal', 'master', 'slave', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT KILL type must be one of %r" % (
                                client_types,))
            args.extend((b'TYPE', _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((b'SKIPME', b'YES'))
            else:
                args.extend((b'SKIPME', b'NO'))
        if _id is not None:
            args.extend((b'ID', _id))
        if addr is not None:
            args.extend((b'ADDR', addr))
        if laddr is not None:
            args.extend((b'LADDR', laddr))
        if user is not None:
            args.extend((b'USER', user))
        if not args:
            raise DataError("CLIENT KILL <filter> <value> ... ... <filter> "
                            "<value> must specify at least one filter")
        return self.execute_command('CLIENT KILL', *args)

    def client_info(self):
        """
        Returns information and statistics about the current
        client connection.

        For more information check https://redis.io/commands/client-info
        """
        return self.execute_command('CLIENT INFO')

    def client_list(self, _type=None, client_id=[]):
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. a list of client ids

        For more information check https://redis.io/commands/client-list
        """
        args = []
        if _type is not None:
            client_types = ('normal', 'master', 'replica', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT LIST _type must be one of %r" % (
                                client_types,))
            args.append(b'TYPE')
            args.append(_type)
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id != []:
            args.append(b"ID")
            args.append(' '.join(client_id))
        return self.execute_command('CLIENT LIST', *args)

    def client_getname(self):
        """
        Returns the current connection name

        For more information check https://redis.io/commands/client-getname
        """
        return self.execute_command('CLIENT GETNAME')

    def client_getredir(self):
        """
        Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return self.execute_command('CLIENT GETREDIR')

    def client_reply(self, reply):
        """
        Enable and disable redis server replies.
        ``reply`` Must be ON OFF or SKIP,
            ON - The default most with server replies to commands
            OFF - Disable server responses to commands
            SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
              The test_client_reply unit test illustrates this, and
              conftest.py has a client with a timeout.

        See https://redis.io/commands/client-reply
        """
        replies = ['ON', 'OFF', 'SKIP']
        if reply not in replies:
            raise DataError('CLIENT REPLY must be one of %r' % replies)
        return self.execute_command("CLIENT REPLY", reply)

    def client_id(self):
        """
        Returns the current connection id

        For more information check https://redis.io/commands/client-id
        """
        return self.execute_command('CLIENT ID')

    def client_trackinginfo(self):
        """
        Returns the information about the current client connection's
        use of the server assisted client side cache.

        See https://redis.io/commands/client-trackinginfo
        """
        return self.execute_command('CLIENT TRACKINGINFO')

    def client_setname(self, name):
        """
        Sets the current connection name

        For more information check https://redis.io/commands/client-setname
        """
        return self.execute_command('CLIENT SETNAME', name)

    def client_unblock(self, client_id, error=False):
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.

        For more information check https://redis.io/commands/client-unblock
        """
        args = ['CLIENT UNBLOCK', int(client_id)]
        if error:
            args.append(b'ERROR')
        return self.execute_command(*args)

    def client_pause(self, timeout):
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients

        For more information check https://redis.io/commands/client-pause
        """
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        return self.execute_command('CLIENT PAUSE', str(timeout))

    def client_unpause(self):
        """
        Unpause all redis clients

        For more information check https://redis.io/commands/client-unpause
        """
        return self.execute_command('CLIENT UNPAUSE')

    def readwrite(self):
        """
        Disables read queries for a connection to a Redis Cluster slave node.

        For more information check https://redis.io/commands/readwrite
        """
        return self.execute_command('READWRITE')

    def readonly(self):
        """
        Enables read queries for a connection to a Redis Cluster replica node.

        For more information check https://redis.io/commands/readonly
        """
        return self.execute_command('READONLY')

    def config_get(self, pattern="*"):
        """
        Return a dictionary of configuration based on the ``pattern``

        For more information check https://redis.io/commands/config-get
        """
        return self.execute_command('CONFIG GET', pattern)

    def config_set(self, name, value):
        """Set config item ``name`` with ``value``

        For more information check https://redis.io/commands/config-set
        """
        return self.execute_command('CONFIG SET', name, value)

    def config_resetstat(self):
        """
        Reset runtime statistics

        For more information check https://redis.io/commands/config-resetstat
        """
        return self.execute_command('CONFIG RESETSTAT')

    def config_rewrite(self):
        """
        Rewrite config file with the minimal change to reflect running config.

        For more information check https://redis.io/commands/config-rewrite
        """
        return self.execute_command('CONFIG REWRITE')

    def dbsize(self):
        """
        Returns the number of keys in the current database

        For more information check https://redis.io/commands/dbsize
        """
        return self.execute_command('DBSIZE')

    def debug_object(self, key):
        """
        Returns version specific meta information about a given key

        For more information check https://redis.io/commands/debug-object
        """
        return self.execute_command('DEBUG OBJECT', key)

    def debug_segfault(self):
        raise NotImplementedError(
            """
            DEBUG SEGFAULT is intentionally not implemented in the client.

            For more information check https://redis.io/commands/debug-segfault
            """
        )

    def echo(self, value):
        """
        Echo the string back from the server

        For more information check https://redis.io/commands/echo
        """
        return self.execute_command('ECHO', value)

    def flushall(self, asynchronous=False):
        """
        Delete all keys in all databases on the current host.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information check https://redis.io/commands/flushall
        """
        args = []
        if asynchronous:
            args.append(b'ASYNC')
        return self.execute_command('FLUSHALL', *args)

    def flushdb(self, asynchronous=False):
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information check https://redis.io/commands/flushdb
        """
        args = []
        if asynchronous:
            args.append(b'ASYNC')
        return self.execute_command('FLUSHDB', *args)

    def swapdb(self, first, second):
        """
        Swap two databases

        For more information check https://redis.io/commands/swapdb
        """
        return self.execute_command('SWAPDB', first, second)

    def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError

        For more information check https://redis.io/commands/info
        """
        if section is None:
            return self.execute_command('INFO')
        else:
            return self.execute_command('INFO', section)

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk

        For more information check https://redis.io/commands/lastsave
        """
        return self.execute_command('LASTSAVE')

    def lolwut(self, *version_numbers):
        """
        Get the Redis version and a piece of generative computer art

        See: https://redis.io/commands/lolwut
        """
        if version_numbers:
            return self.execute_command('LOLWUT VERSION', *version_numbers)
        else:
            return self.execute_command('LOLWUT')

    def migrate(self, host, port, keys, destination_db, timeout,
                copy=False, replace=False, auth=None):
        """
        Migrate 1 or more keys from the current Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the connection between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.

        For more information check https://redis.io/commands/migrate
        """
        keys = list_or_args(keys, [])
        if not keys:
            raise DataError('MIGRATE requires at least one key')
        pieces = []
        if copy:
            pieces.append(b'COPY')
        if replace:
            pieces.append(b'REPLACE')
        if auth:
            pieces.append(b'AUTH')
            pieces.append(auth)
        pieces.append(b'KEYS')
        pieces.extend(keys)
        return self.execute_command('MIGRATE', host, port, '', destination_db,
                                    timeout, *pieces)

    def object(self, infotype, key):
        """
        Return the encoding, idletime, or refcount about the key
        """
        return self.execute_command('OBJECT', infotype, key, infotype=infotype)

    def memory_doctor(self):
        raise NotImplementedError(
            """
            MEMORY DOCTOR is intentionally not implemented in the client.

            For more information check https://redis.io/commands/memory-doctor
            """
        )

    def memory_help(self):
        raise NotImplementedError(
            """
            MEMORY HELP is intentionally not implemented in the client.

            For more information check https://redis.io/commands/memory-help
            """
        )

    def memory_stats(self):
        """
        Return a dictionary of memory stats

        For more information check https://redis.io/commands/memory-stats
        """
        return self.execute_command('MEMORY STATS')

    def memory_malloc_stats(self):
        """
        Return an internal statistics report from the memory allocator.

        See: https://redis.io/commands/memory-malloc-stats
        """
        return self.execute_command('MEMORY MALLOC-STATS')

    def memory_usage(self, key, samples=None):
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.

        For more information check https://redis.io/commands/memory-usage
        """
        args = []
        if isinstance(samples, int):
            args.extend([b'SAMPLES', samples])
        return self.execute_command('MEMORY USAGE', key, *args)

    def memory_purge(self):
        """
        Attempts to purge dirty pages for reclamation by allocator

        For more information check https://redis.io/commands/memory-purge
        """
        return self.execute_command('MEMORY PURGE')

    def ping(self):
        """
        Ping the Redis server

        For more information check https://redis.io/commands/ping
        """
        return self.execute_command('PING')

    def quit(self):
        """
        Ask the server to close the connection.

        For more information check https://redis.io/commands/quit
        """
        return self.execute_command('QUIT')

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete

        For more information check https://redis.io/commands/save
        """
        return self.execute_command('SAVE')

    def shutdown(self, save=False, nosave=False):
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.

        For more information check https://redis.io/commands/shutdown
        """
        if save and nosave:
            raise DataError('SHUTDOWN save and nosave cannot both be set')
        args = ['SHUTDOWN']
        if save:
            args.append('SAVE')
        if nosave:
            args.append('NOSAVE')
        try:
            self.execute_command(*args)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slaveof(self, host=None, port=None):
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.

        For more information check https://redis.io/commands/slaveof
        """
        if host is None and port is None:
            return self.execute_command('SLAVEOF', b'NO', b'ONE')
        return self.execute_command('SLAVEOF', host, port)

    def slowlog_get(self, num=None):
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.

        For more information check https://redis.io/commands/slowlog-get
        """
        args = ['SLOWLOG GET']
        if num is not None:
            args.append(num)
        decode_responses = self.connection_pool.connection_kwargs.get(
            'decode_responses', False)
        return self.execute_command(*args, decode_responses=decode_responses)

    def slowlog_len(self):
        """
        Get the number of items in the slowlog

        For more information check https://redis.io/commands/slowlog-len
        """
        return self.execute_command('SLOWLOG LEN')

    def slowlog_reset(self):
        """
        Remove all items in the slowlog

        For more information check https://redis.io/commands/slowlog-reset
        """
        return self.execute_command('SLOWLOG RESET')

    def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).

        For more information check https://redis.io/commands/time
        """
        return self.execute_command('TIME')

    def wait(self, num_replicas, timeout):
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.

        For more information check https://redis.io/commands/wait
        """
        return self.execute_command('WAIT', num_replicas, timeout)

    # BASIC KEY COMMANDS
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.

        For more information check https://redis.io/commands/append
        """
        return self.execute_command('APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` parameters indicate which bytes to consider

        For more information check https://redis.io/commands/bitcount
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or \
                (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        return self.execute_command('BITCOUNT', *params)

    def bitfield(self, key, default_overflow=None):
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.

        For more information check https://redis.io/commands/bitfield
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.

        For more information check https://redis.io/commands/bitop
        """
        return self.execute_command('BITOP', operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` defines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.

        For more information check https://redis.io/commands/bitpos
        """
        if bit not in (0, 1):
            raise DataError('bit must be 0 or 1')
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise DataError("start argument is not set, "
                            "when end is specified")
        return self.execute_command('BITPOS', *params)

    def copy(self, source, destination, destination_db=None, replace=False):
        """
        Copy the value stored in the ``source`` key to the ``destination`` key.

        ``destination_db`` an alternative destination database. By default,
        the ``destination`` key is created in the source Redis database.

        ``replace`` whether the ``destination`` key should be removed before
        copying the value to it. By default, the value is not copied if
        the ``destination`` key already exists.

        For more information check https://redis.io/commands/copy
        """
        params = [source, destination]
        if destination_db is not None:
            params.extend(["DB", destination_db])
        if replace:
            params.append("REPLACE")
        return self.execute_command('COPY', *params)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``

        For more information check https://redis.io/commands/decr
        """
        # An alias for ``decr()``, because it is already implemented
        # as DECRBY redis command.
        return self.decrby(name, amount)

    def decrby(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``

        For more information check https://redis.io/commands/decrby
        """
        return self.execute_command('DECRBY', name, amount)

    def delete(self, *names):
        """
        Delete one or more keys specified by ``names``
        """
        return self.execute_command('DEL', *names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.

        For more information check https://redis.io/commands/dump
        """
        return self.execute_command('DUMP', name)

    def exists(self, *names):
        """
        Returns the number of ``names`` that exist

        For more information check https://redis.io/commands/exists
        """
        return self.execute_command('EXISTS', *names)
    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.

        For more information check https://redis.io/commands/expire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.

        For more information check https://redis.io/commands/expireat
        """
        if isinstance(when, datetime.datetime):
            when = int(time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist

        For more information check https://redis.io/commands/get
        """
        return self.execute_command('GET', name)

    def getdel(self, name):
        """
        Get the value at key ``name`` and delete the key. This command
        is similar to GET, except for the fact that it also deletes
        the key on success (if and only if the key's value type
        is a string).

        For more information check https://redis.io/commands/getdel
        """
        return self.execute_command('GETDEL', name)

    def getex(self, name,
              ex=None, px=None, exat=None, pxat=None, persist=False):
        """
        Get the value of key and optionally set its expiration.
        GETEX is similar to GET, but is a write command with
        additional options. All time parameters can be given as
        datetime.timedelta or integers.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
        specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
        specified in unix time.

        ``persist`` remove the time to live associated with ``name``.

        For more information check https://redis.io/commands/getex
        """

        opset = set([ex, px, exat, pxat])
        if len(opset) > 2 or len(opset) > 1 and persist:
            raise DataError("``ex``, ``px``, ``exat``, ``pxat``, "
                            "and ``persist`` are mutually exclusive.")

        pieces = []
        # similar to set command
        if ex is not None:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)
        if px is not None:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)
        # similar to pexpireat command
        if exat is not None:
            pieces.append('EXAT')
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append('PXAT')
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if persist:
            pieces.append('PERSIST')

        return self.execute_command('GETEX', name, *pieces)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        """
        Returns a boolean indicating the value of ``offset`` in ``name``

        For more information check https://redis.io/commands/getbit
        """
        return self.execute_command('GETBIT', name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)

        For more information check https://redis.io/commands/getrange
        """
        return self.execute_command('GETRANGE', key, start, end)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.

        As per Redis 6.2, GETSET is considered deprecated.
        Please use SET with GET parameter in new code.

        For more information check https://redis.io/commands/getset
        """
        return self.execute_command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``

        For more information check https://redis.io/commands/incr
        """
        return self.incrby(name, amount)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``

        For more information check https://redis.io/commands/incrby
        """
        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.execute_command('INCRBY', name, amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``

        For more information check https://redis.io/commands/incrbyfloat
        """
        return self.execute_command('INCRBYFLOAT', name, amount)

    def keys(self, pattern='*'):
        """
        Returns a list of keys matching ``pattern``

        For more information check https://redis.io/commands/keys
        """
        return self.execute_command('KEYS', pattern)

    def lmove(self, first_list, second_list, src="LEFT", dest="RIGHT"):
        """
        Atomically returns and removes the first/last element of a list,
        pushing it as the first/last element on the destination list.
        Returns the element being popped and pushed.

        For more information check https://redis.io/commands/lmov
        """
        params = [first_list, second_list, src, dest]
        return self.execute_command("LMOVE", *params)

    def blmove(self, first_list, second_list, timeout,
               src="LEFT", dest="RIGHT"):
        """
        Blocking version of lmove.

        For more information check https://redis.io/commands/blmove
        """
        params = [first_list, second_list, src, dest, timeout]
        return self.execute_command("BLMOVE", *params)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        For more information check https://redis.io/commands/mget
        """
        from redis.client import EMPTY_RESPONSE
        args = list_or_args(keys, args)
        options = {}
        if not args:
            options[EMPTY_RESPONSE] = []
        return self.execute_command('MGET', *args, **options)

    def mset(self, mapping):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().

        For more information check https://redis.io/commands/mset
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command('MSET', *items)

    def msetnx(self, mapping):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.

        For more information check https://redis.io/commands/msetnx
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        """
        Moves the key ``name`` to a different Redis database ``db``

        For more information check https://redis.io/commands/move
        """
        return self.execute_command('MOVE', name, db)

    def persist(self, name):
        """
        Removes an expiration on ``name``

        For more information check https://redis.io/commands/persist
        """
        return self.execute_command('PERSIST', name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.

        For more information check https://redis.io/commands/pexpire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds() * 1000)
        return self.execute_command('PEXPIRE', name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.

        For more information check https://redis.io/commands/pexpireat
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command('PEXPIREAT', name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object

        For more information check https://redis.io/commands/psetex
        """
        if isinstance(time_ms, datetime.timedelta):
            time_ms = int(time_ms.total_seconds() * 1000)
        return self.execute_command('PSETEX', name, time_ms, value)

    def pttl(self, name):
        """
        Returns the number of milliseconds until the key ``name`` will expire

        For more information check https://redis.io/commands/pttl
        """
        return self.execute_command('PTTL', name)

    def hrandfield(self, key, count=None, withvalues=False):
        """
        Return a random field from the hash value stored at key.

        count: if the argument is positive, return an array of distinct fields.
        If called with a negative count, the behavior changes and the command
        is allowed to return the same field multiple times. In this case,
        the number of returned fields is the absolute value of the
        specified count.
        withvalues: The optional WITHVALUES modifier changes the reply so it
        includes the respective values of the randomly selected hash fields.

        For more information check https://redis.io/commands/hrandfield
        """
        params = []
        if count is not None:
            params.append(count)
        if withvalues:
            params.append("WITHVALUES")

        return self.execute_command("HRANDFIELD", key, *params)

    def randomkey(self):
        """
        Returns the name of a random key

        For more information check https://redis.io/commands/randomkey
        """
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``

        For more information check https://redis.io/commands/rename
        """
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        """
        Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist

        For more information check https://redis.io/commands/renamenx
        """
        return self.execute_command('RENAMENX', src, dst)

    def restore(self, name, ttl, value, replace=False, absttl=False,
                idletime=None, frequency=None):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.

        ``replace`` allows an existing key on ``name`` to be overridden. If
        it's not specified an error is raised on collision.

        ``absttl`` if True, specified ``ttl`` should represent an absolute Unix
        timestamp in milliseconds in which the key will expire. (Redis 5.0 or
        greater).

        ``idletime`` Used for eviction, this is the number of seconds the
        key must be idle, prior to execution.

        ``frequency`` Used for eviction, this is the frequency counter of
        the object stored at the key, prior to execution.

        For more information check https://redis.io/commands/restore
        """
        params = [name, ttl, value]
        if replace:
            params.append('REPLACE')
        if absttl:
            params.append('ABSTTL')
        if idletime is not None:
            params.append('IDLETIME')
            try:
                params.append(int(idletime))
            except ValueError:
                raise DataError("idletimemust be an integer")

        if frequency is not None:
            params.append('FREQ')
            try:
                params.append(int(frequency))
            except ValueError:
                raise DataError("frequency must be an integer")

        return self.execute_command('RESTORE', *params)

    def set(self, name, value,
            ex=None, px=None, nx=False, xx=False, keepttl=False, get=False,
            exat=None, pxat=None):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.

        ``keepttl`` if True, retain the time to live associated with the key.
            (Available since Redis 6.0)

        ``get`` if True, set the value at key ``name`` to ``value`` and return
            the old value stored at key, or None if the key did not exist.
            (Available since Redis 6.2)

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
            specified in unix time.

        For more information check https://redis.io/commands/set
        """
        pieces = [name, value]
        options = {}
        if ex is not None:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append('EXAT')
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append('PXAT')
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if keepttl:
            pieces.append('KEEPTTL')

        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')

        if get:
            pieces.append('GET')
            options["get"] = True

        return self.execute_command('SET', *pieces, **options)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.

        For more information check https://redis.io/commands/setbit
        """
        value = value and 1 or 0
        return self.execute_command('SETBIT', name, offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.

        For more information check https://redis.io/commands/setex
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command('SETEX', name, time, value)

    def setnx(self, name, value):
        """
        Set the value of key ``name`` to ``value`` if key doesn't exist

        For more information check https://redis.io/commands/setnx
        """
        return self.execute_command('SETNX', name, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.

        For more information check https://redis.io/commands/setrange
        """
        return self.execute_command('SETRANGE', name, offset, value)

    def stralgo(self, algo, value1, value2, specific_argument='strings',
                len=False, idx=False, minmatchlen=None, withmatchlen=False):
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.

        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.

        For more information check https://redis.io/commands/stralgo
        """
        # check validity
        supported_algo = ['LCS']
        if algo not in supported_algo:
            raise DataError("The supported algorithms are: %s"
                            % (', '.join(supported_algo)))
        if specific_argument not in ['keys', 'strings']:
            raise DataError("specific_argument can be only"
                            " keys or strings")
        if len and idx:
            raise DataError("len and idx cannot be provided together.")

        pieces = [algo, specific_argument.upper(), value1, value2]
        if len:
            pieces.append(b'LEN')
        if idx:
            pieces.append(b'IDX')
        try:
            int(minmatchlen)
            pieces.extend([b'MINMATCHLEN', minmatchlen])
        except TypeError:
            pass
        if withmatchlen:
            pieces.append(b'WITHMATCHLEN')

        return self.execute_command('STRALGO', *pieces, len=len, idx=idx,
                                    minmatchlen=minmatchlen,
                                    withmatchlen=withmatchlen)

    def strlen(self, name):
        """
        Return the number of bytes stored in the value of ``name``

        For more information check https://redis.io/commands/strlen
        """
        return self.execute_command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)

    def touch(self, *args):
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.

        For more information check https://redis.io/commands/touch
        """
        return self.execute_command('TOUCH', *args)

    def ttl(self, name):
        """
        Returns the number of seconds until the key ``name`` will expire

        For more information check https://redis.io/commands/ttl
        """
        return self.execute_command('TTL', name)

    def type(self, name):
        """
        Returns the type of key ``name``

        For more information check https://redis.io/commands/type
        """
        return self.execute_command('TYPE', name)

    def watch(self, *names):
        """
        Watches the values at keys ``names``, or None if the key doesn't exist

        For more information check https://redis.io/commands/type
        """
        warnings.warn(DeprecationWarning('Call WATCH from a Pipeline object'))

    def unwatch(self):
        """
        Unwatches the value at key ``name``, or None of the key doesn't exist

        For more information check https://redis.io/commands/unwatch
        """
        warnings.warn(
            DeprecationWarning('Call UNWATCH from a Pipeline object'))

    def unlink(self, *names):
        """
        Unlink one or more keys specified by ``names``

        For more information check https://redis.io/commands/unlink
        """
        return self.execute_command('UNLINK', *names)

    # LIST COMMANDS
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/blpop
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/brpop
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BRPOP', *keys)

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.

        For more information check https://redis.io/commands/brpoplpush
        """
        if timeout is None:
            timeout = 0
        return self.execute_command('BRPOPLPUSH', src, dst, timeout)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list

        For more information check https://redis.io/commands/lindex
        """
        return self.execute_command('LINDEX', name, index)

    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.

        For more information check https://redis.io/commands/linsert
        """
        return self.execute_command('LINSERT', name, where, refvalue, value)

    def llen(self, name):
        """
        Return the length of the list ``name``

        For more information check https://redis.io/commands/llen
        """
        return self.execute_command('LLEN', name)

    def lpop(self, name, count=None):
        """
        Removes and returns the first elements of the list ``name``.

        By default, the command pops a single element from the beginning of
        the list. When provided with the optional ``count`` argument, the reply
        will consist of up to count elements, depending on the list's length.

        For more information check https://redis.io/commands/lpop
        """
        if count is not None:
            return self.execute_command('LPOP', name, count)
        else:
            return self.execute_command('LPOP', name)

    def lpush(self, name, *values):
        """
        Push ``values`` onto the head of the list ``name``

        For more information check https://redis.io/commands/lpush
        """
        return self.execute_command('LPUSH', name, *values)

    def lpushx(self, name, *values):
        """
        Push ``value`` onto the head of the list ``name`` if ``name`` exists

        For more information check https://redis.io/commands/lpushx
        """
        return self.execute_command('LPUSHX', name, *values)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information check https://redis.io/commands/lrange
        """
        return self.execute_command('LRANGE', name, start, end)

    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

            For more information check https://redis.io/commands/lrem
        """
        return self.execute_command('LREM', name, count, value)

    def lset(self, name, index, value):
        """
        Set ``position`` of list ``name`` to ``value``

        For more information check https://redis.io/commands/lset
        """
        return self.execute_command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information check https://redis.io/commands/ltrim
        """
        return self.execute_command('LTRIM', name, start, end)

    def rpop(self, name, count=None):
        """
        Removes and returns the last elements of the list ``name``.

        By default, the command pops a single element from the end of the list.
        When provided with the optional ``count`` argument, the reply will
        consist of up to count elements, depending on the list's length.

        For more information check https://redis.io/commands/rpop
        """
        if count is not None:
            return self.execute_command('RPOP', name, count)
        else:
            return self.execute_command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.

        For more information check https://redis.io/commands/rpoplpush
        """
        return self.execute_command('RPOPLPUSH', src, dst)

    def rpush(self, name, *values):
        """
        Push ``values`` onto the tail of the list ``name``

        For more information check https://redis.io/commands/rpush
        """
        return self.execute_command('RPUSH', name, *values)

    def rpushx(self, name, value):
        """
        Push ``value`` onto the tail of the list ``name`` if ``name`` exists

        For more information check https://redis.io/commands/rpushx
        """
        return self.execute_command('RPUSHX', name, value)

    def lpos(self, name, value, rank=None, count=None, maxlen=None):
        """
        Get position of ``value`` within the list ``name``

         If specified, ``rank`` indicates the "rank" of the first element to
         return in case there are multiple copies of ``value`` in the list.
         By default, LPOS returns the position of the first occurrence of
         ``value`` in the list. When ``rank`` 2, LPOS returns the position of
         the second ``value`` in the list. If ``rank`` is negative, LPOS
         searches the list in reverse. For example, -1 would return the
         position of the last occurrence of ``value`` and -2 would return the
         position of the next to last occurrence of ``value``.

         If specified, ``count`` indicates that LPOS should return a list of
         up to ``count`` positions. A ``count`` of 2 would return a list of
         up to 2 positions. A ``count`` of 0 returns a list of all positions
         matching ``value``. When ``count`` is specified and but ``value``
         does not exist in the list, an empty list is returned.

         If specified, ``maxlen`` indicates the maximum number of list
         elements to scan. A ``maxlen`` of 1000 will only return the
         position(s) of items within the first 1000 entries in the list.
         A ``maxlen`` of 0 (the default) will scan the entire list.

         For more information check https://redis.io/commands/lpos
        """
        pieces = [name, value]
        if rank is not None:
            pieces.extend(['RANK', rank])

        if count is not None:
            pieces.extend(['COUNT', count])

        if maxlen is not None:
            pieces.extend(['MAXLEN', maxlen])

        return self.execute_command('LPOS', *pieces)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        For more information check https://redis.io/commands/sort
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.extend([b'BY', by])
        if start is not None and num is not None:
            pieces.extend([b'LIMIT', start, num])
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, (bytes, str)):
                pieces.extend([b'GET', get])
            else:
                for g in get:
                    pieces.extend([b'GET', g])
        if desc:
            pieces.append(b'DESC')
        if alpha:
            pieces.append(b'ALPHA')
        if store is not None:
            pieces.extend([b'STORE', store])
        if groups:
            if not get or isinstance(get, (bytes, str)) or len(get) < 2:
                raise DataError('when using "groups" the "get" argument '
                                'must be specified and contain at least '
                                'two keys')

        options = {'groups': len(get) if groups else None}
        return self.execute_command('SORT', *pieces, **options)

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None, _type=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.

        For more information check https://redis.io/commands/scan
        """
        pieces = [cursor]
        if match is not None:
            pieces.extend([b'MATCH', match])
        if count is not None:
            pieces.extend([b'COUNT', count])
        if _type is not None:
            pieces.extend([b'TYPE', _type])
        return self.execute_command('SCAN', *pieces)

    def scan_iter(self, match=None, count=None, _type=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match,
                                     count=count, _type=_type)
            yield from data

    def sscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        For more information check https://redis.io/commands/sscan
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([b'MATCH', match])
        if count is not None:
            pieces.extend([b'COUNT', count])
        return self.execute_command('SSCAN', *pieces)

    def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            yield from data

    def hscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        For more information check https://redis.io/commands/hscan
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([b'MATCH', match])
        if count is not None:
            pieces.extend([b'COUNT', count])
        return self.execute_command('HSCAN', *pieces)

    def hscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            yield from data.items()

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zscan
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([b'MATCH', match])
        if count is not None:
            pieces.extend([b'COUNT', count])
        options = {'score_cast_func': score_cast_func}
        return self.execute_command('ZSCAN', *pieces, **options)

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            yield from data

    # SET COMMANDS
    def sadd(self, name, *values):
        """
        Add ``value(s)`` to set ``name``

        For more information check https://redis.io/commands/sadd
        """
        return self.execute_command('SADD', name, *values)

    def scard(self, name):
        """
        Return the number of elements in set ``name``

        For more information check https://redis.io/commands/scard
        """
        return self.execute_command('SCARD', name)

    def sdiff(self, keys, *args):
        """
        Return the difference of sets specified by ``keys``

        For more information check https://redis.io/commands/sdiff
        """
        args = list_or_args(keys, args)
        return self.execute_command('SDIFF', *args)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sdiffstore
        """
        args = list_or_args(keys, args)
        return self.execute_command('SDIFFSTORE', dest, *args)

    def sinter(self, keys, *args):
        """
        Return the intersection of sets specified by ``keys``

        For more information check https://redis.io/commands/sinter
        """
        args = list_or_args(keys, args)
        return self.execute_command('SINTER', *args)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sinterstore
        """
        args = list_or_args(keys, args)
        return self.execute_command('SINTERSTORE', dest, *args)

    def sismember(self, name, value):
        """
        Return a boolean indicating if ``value`` is a member of set ``name``

        For more information check https://redis.io/commands/sismember
        """
        return self.execute_command('SISMEMBER', name, value)

    def smembers(self, name):
        """
        Return all members of the set ``name``

        For more information check https://redis.io/commands/smembers
        """
        return self.execute_command('SMEMBERS', name)

    def smismember(self, name, values, *args):
        """
        Return whether each value in ``values`` is a member of the set ``name``
        as a list of ``bool`` in the order of ``values``

        For more information check https://redis.io/commands/smismember
        """
        args = list_or_args(values, args)
        return self.execute_command('SMISMEMBER', name, *args)

    def smove(self, src, dst, value):
        """
        Move ``value`` from set ``src`` to set ``dst`` atomically

        For more information check https://redis.io/commands/smove
        """
        return self.execute_command('SMOVE', src, dst, value)

    def spop(self, name, count=None):
        """
        Remove and return a random member of set ``name``

        For more information check https://redis.io/commands/spop
        """
        args = (count is not None) and [count] or []
        return self.execute_command('SPOP', name, *args)

    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``. Note this is only available when running
        Redis 2.6+.

        For more information check https://redis.io/commands/srandmember
        """
        args = (number is not None) and [number] or []
        return self.execute_command('SRANDMEMBER', name, *args)

    def srem(self, name, *values):
        """
        Remove ``values`` from set ``name``

        For more information check https://redis.io/commands/srem
        """
        return self.execute_command('SREM', name, *values)

    def sunion(self, keys, *args):
        """
        Return the union of sets specified by ``keys``

        For more information check https://redis.io/commands/sunion
        """
        args = list_or_args(keys, args)
        return self.execute_command('SUNION', *args)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sunionstore
        """
        args = list_or_args(keys, args)
        return self.execute_command('SUNIONSTORE', dest, *args)

    # STREAMS COMMANDS
    def xack(self, name, groupname, *ids):
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowledge.

        For more information check https://redis.io/commands/xack
        """
        return self.execute_command('XACK', name, groupname, *ids)

    def xadd(self, name, fields, id='*', maxlen=None, approximate=True,
             nomkstream=False, minid=None, limit=None):
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size.
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        nomkstream: When set to true, do not make a stream
        minid: the minimum id in the stream to query.
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve

        For more information check https://redis.io/commands/xadd
        """
        pieces = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ```maxlen``` or ```minid``` "
                            "may be specified")

        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise DataError('XADD maxlen must be a positive integer')
            pieces.append(b'MAXLEN')
            if approximate:
                pieces.append(b'~')
            pieces.append(str(maxlen))
        if minid is not None:
            pieces.append(b'MINID')
            if approximate:
                pieces.append(b'~')
            pieces.append(minid)
        if limit is not None:
            pieces.extend([b'LIMIT', limit])
        if nomkstream:
            pieces.append(b'NOMKSTREAM')
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError('XADD fields must be a non-empty dict')
        for pair in fields.items():
            pieces.extend(pair)
        return self.execute_command('XADD', name, *pieces)

    def xautoclaim(self, name, groupname, consumername, min_idle_time,
                   start_id=0, count=None, justid=False):
        """
        Transfers ownership of pending stream entries that match the specified
        criteria. Conceptually, equivalent to calling XPENDING and then XCLAIM,
        but provides a more straightforward way to deal with message delivery
        failures via SCAN-like semantics.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds.
        start_id: filter messages with equal or greater ID.
        count: optional integer, upper limit of the number of entries that the
        command attempts to claim. Set to 100 by default.
        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message

        For more information check https://redis.io/commands/xautoclaim
        """
        try:
            if int(min_idle_time) < 0:
                raise DataError("XAUTOCLAIM min_idle_time must be a non"
                                "negative integer")
        except TypeError:
            pass

        kwargs = {}
        pieces = [name, groupname, consumername, min_idle_time, start_id]

        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([b'COUNT', count])
        except TypeError:
            pass
        if justid:
            pieces.append(b'JUSTID')
            kwargs['parse_justid'] = True

        return self.execute_command('XAUTOCLAIM', *pieces, **kwargs)

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids,
               idle=None, time=None, retrycount=None, force=False,
               justid=False):
        """
        Changes the ownership of a pending message.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds
        message_ids: non-empty list or tuple of message IDs to claim
        idle: optional. Set the idle time (last time it was delivered) of the
         message in ms
        time: optional integer. This is the same as idle but instead of a
         relative amount of milliseconds, it sets the idle time to a specific
         Unix time (in milliseconds).
        retrycount: optional integer. set the retry counter to the specified
         value. This counter is incremented every time a message is delivered
         again.
        force: optional boolean, false by default. Creates the pending message
         entry in the PEL even if certain specified IDs are not already in the
         PEL assigned to a different client.
        justid: optional boolean, false by default. Return just an array of IDs
         of messages successfully claimed, without returning the actual message

         For more information check https://redis.io/commands/xclaim
        """
        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise DataError("XCLAIM min_idle_time must be a non negative "
                            "integer")
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise DataError("XCLAIM message_ids must be a non empty list or "
                            "tuple of message IDs to claim")

        kwargs = {}
        pieces = [name, groupname, consumername, str(min_idle_time)]
        pieces.extend(list(message_ids))

        if idle is not None:
            if not isinstance(idle, int):
                raise DataError("XCLAIM idle must be an integer")
            pieces.extend((b'IDLE', str(idle)))
        if time is not None:
            if not isinstance(time, int):
                raise DataError("XCLAIM time must be an integer")
            pieces.extend((b'TIME', str(time)))
        if retrycount is not None:
            if not isinstance(retrycount, int):
                raise DataError("XCLAIM retrycount must be an integer")
            pieces.extend((b'RETRYCOUNT', str(retrycount)))

        if force:
            if not isinstance(force, bool):
                raise DataError("XCLAIM force must be a boolean")
            pieces.append(b'FORCE')
        if justid:
            if not isinstance(justid, bool):
                raise DataError("XCLAIM justid must be a boolean")
            pieces.append(b'JUSTID')
            kwargs['parse_justid'] = True
        return self.execute_command('XCLAIM', *pieces, **kwargs)

    def xdel(self, name, *ids):
        """
        Deletes one or more messages from a stream.
        name: name of the stream.
        *ids: message ids to delete.

        For more information check https://redis.io/commands/xdel
        """
        return self.execute_command('XDEL', name, *ids)

    def xgroup_create(self, name, groupname, id='$', mkstream=False):
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information check https://redis.io/commands/xgroup-create
        """
        pieces = ['XGROUP CREATE', name, groupname, id]
        if mkstream:
            pieces.append(b'MKSTREAM')
        return self.execute_command(*pieces)

    def xgroup_delconsumer(self, name, groupname, consumername):
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete

        For more information check https://redis.io/commands/xgroup-delconsumer
        """
        return self.execute_command('XGROUP DELCONSUMER', name, groupname,
                                    consumername)

    def xgroup_destroy(self, name, groupname):
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xgroup-destroy
        """
        return self.execute_command('XGROUP DESTROY', name, groupname)

    def xgroup_createconsumer(self, name, groupname, consumername):
        """
        Consumers in a consumer group are auto-created every time a new
        consumer name is mentioned by some command.
        They can be explicitly created by using this command.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to create.

        See: https://redis.io/commands/xgroup-createconsumer
        """
        return self.execute_command('XGROUP CREATECONSUMER', name, groupname,
                                    consumername)

    def xgroup_setid(self, name, groupname, id):
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information check https://redis.io/commands/xgroup-setid
        """
        return self.execute_command('XGROUP SETID', name, groupname, id)

    def xinfo_consumers(self, name, groupname):
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xinfo-consumers
        """
        return self.execute_command('XINFO CONSUMERS', name, groupname)

    def xinfo_groups(self, name):
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.

        For more information check https://redis.io/commands/xinfo-groups
        """
        return self.execute_command('XINFO GROUPS', name)

    def xinfo_stream(self, name, full=False):
        """
        Returns general information about the stream.
        name: name of the stream.
        full: optional boolean, false by default. Return full summary

        For more information check https://redis.io/commands/xinfo-stream
        """
        pieces = [name]
        options = {}
        if full:
            pieces.append(b'FULL')
            options = {'full': full}
        return self.execute_command('XINFO STREAM', *pieces, **options)

    def xlen(self, name):
        """
        Returns the number of elements in a given stream.

        For more information check https://redis.io/commands/xlen
        """
        return self.execute_command('XLEN', name)

    def xpending(self, name, groupname):
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xpending
        """
        return self.execute_command('XPENDING', name, groupname)

    def xpending_range(self, name, groupname, idle=None,
                       min=None, max=None, count=None,
                       consumername=None):
        """
        Returns information about pending messages, in a range.

        name: name of the stream.
        groupname: name of the consumer group.
        idle: available from  version 6.2. filter entries by their
        idle-time, given in milliseconds (optional).
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).
        """
        if {min, max, count} == {None}:
            if idle is not None or consumername is not None:
                raise DataError("if XPENDING is provided with idle time"
                                " or consumername, it must be provided"
                                " with min, max and count parameters")
            return self.xpending(name, groupname)

        pieces = [name, groupname]
        if min is None or max is None or count is None:
            raise DataError("XPENDING must be provided with min, max "
                            "and count parameters, or none of them.")
        # idle
        try:
            if int(idle) < 0:
                raise DataError("XPENDING idle must be a integer >= 0")
            pieces.extend(['IDLE', idle])
        except TypeError:
            pass
        # count
        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([min, max, count])
        except TypeError:
            pass
        # consumername
        if consumername:
            pieces.append(consumername)

        return self.execute_command('XPENDING', *pieces, parse_detail=True)

    def xrange(self, name, min='-', max='+', count=None):
        """
        Read stream values within an interval.
        name: name of the stream.
        start: first stream ID. defaults to '-',
               meaning the earliest available.
        finish: last stream ID. defaults to '+',
                meaning the latest available.
        count: if set, only return this many items, beginning with the
               earliest available.

        For more information check https://redis.io/commands/xrange
        """
        pieces = [min, max]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError('XRANGE count must be a positive integer')
            pieces.append(b'COUNT')
            pieces.append(str(count))

        return self.execute_command('XRANGE', name, *pieces)

    def xread(self, streams, count=None, block=None):
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.

        For more information check https://redis.io/commands/xread
        """
        pieces = []
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError('XREAD block must be a non-negative integer')
            pieces.append(b'BLOCK')
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError('XREAD count must be a positive integer')
            pieces.append(b'COUNT')
            pieces.append(str(count))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError('XREAD streams must be a non empty dict')
        pieces.append(b'STREAMS')
        keys, values = zip(*streams.items())
        pieces.extend(keys)
        pieces.extend(values)
        return self.execute_command('XREAD', *pieces)

    def xreadgroup(self, groupname, consumername, streams, count=None,
                   block=None, noack=False):
        """
        Read from a stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL

        For more information check https://redis.io/commands/xreadgroup
        """
        pieces = [b'GROUP', groupname, consumername]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append(b'COUNT')
            pieces.append(str(count))
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative "
                                "integer")
            pieces.append(b'BLOCK')
            pieces.append(str(block))
        if noack:
            pieces.append(b'NOACK')
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError('XREADGROUP streams must be a non empty dict')
        pieces.append(b'STREAMS')
        pieces.extend(streams.keys())
        pieces.extend(streams.values())
        return self.execute_command('XREADGROUP', *pieces)

    def xrevrange(self, name, max='+', min='-', count=None):
        """
        Read stream values within an interval, in reverse order.
        name: name of the stream
        start: first stream ID. defaults to '+',
               meaning the latest available.
        finish: last stream ID. defaults to '-',
                meaning the earliest available.
        count: if set, only return this many items, beginning with the
               latest available.

        For more information check https://redis.io/commands/xrevrange
        """
        pieces = [max, min]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError('XREVRANGE count must be a positive integer')
            pieces.append(b'COUNT')
            pieces.append(str(count))

        return self.execute_command('XREVRANGE', name, *pieces)

    def xtrim(self, name, maxlen=None, approximate=True, minid=None,
              limit=None):
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        minid: the minimum id in the stream to query
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve

        For more information check https://redis.io/commands/xtrim
        """
        pieces = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` "
                            "may be specified")

        if maxlen is not None:
            pieces.append(b'MAXLEN')
        if minid is not None:
            pieces.append(b'MINID')
        if approximate:
            pieces.append(b'~')
        if maxlen is not None:
            pieces.append(maxlen)
        if minid is not None:
            pieces.append(minid)
        if limit is not None:
            pieces.append(b"LIMIT")
            pieces.append(limit)

        return self.execute_command('XTRIM', name, *pieces)

    # SORTED SET COMMANDS
    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False,
             gt=None, lt=None):
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        ``LT`` Only update existing elements if the new score is less than
        the current score. This flag doesn't prevent adding new elements.

        ``GT`` Only update existing elements if the new score is greater than
        the current score. This flag doesn't prevent adding new elements.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.

        ``NX``, ``LT``, and ``GT`` are mutually exclusive options.

        See: https://redis.io/commands/ZADD
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError("ZADD option 'incr' only works when passing a "
                            "single element/score pair")
        if nx is True and (gt is not None or lt is not None):
            raise DataError("Only one of 'nx', 'lt', or 'gr' may be defined.")

        pieces = []
        options = {}
        if nx:
            pieces.append(b'NX')
        if xx:
            pieces.append(b'XX')
        if ch:
            pieces.append(b'CH')
        if incr:
            pieces.append(b'INCR')
            options['as_score'] = True
        if gt:
            pieces.append(b'GT')
        if lt:
            pieces.append(b'LT')
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command('ZADD', name, *pieces, **options)

    def zcard(self, name):
        """
        Return the number of elements in the sorted set ``name``

        For more information check https://redis.io/commands/zcard
        """
        return self.execute_command('ZCARD', name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.

        For more information check https://redis.io/commands/zcount
        """
        return self.execute_command('ZCOUNT', name, min, max)

    def zdiff(self, keys, withscores=False):
        """
        Returns the difference between the first and all successive input
        sorted sets provided in ``keys``.

        For more information check https://redis.io/commands/zdiff
        """
        pieces = [len(keys), *keys]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("ZDIFF", *pieces)

    def zdiffstore(self, dest, keys):
        """
        Computes the difference between the first and all successive input
        sorted sets provided in ``keys`` and stores the result in ``dest``.

        For more information check https://redis.io/commands/zdiffstore
        """
        pieces = [len(keys), *keys]
        return self.execute_command("ZDIFFSTORE", dest, *pieces)

    def zincrby(self, name, amount, value):
        """
        Increment the score of ``value`` in sorted set ``name`` by ``amount``

        For more information check https://redis.io/commands/zincrby
        """
        return self.execute_command('ZINCRBY', name, amount, value)

    def zinter(self, keys, aggregate=None, withscores=False):
        """
        Return the intersect of multiple sorted sets specified by ``keys``.
        With the ``aggregate`` option, it is possible to specify how the
        results of the union are aggregated. This option defaults to SUM,
        where the score of an element is summed across the inputs where it
        exists. When this option is set to either MIN or MAX, the resulting
        set will contain the minimum or maximum score of an element across
        the inputs where it exists.

        For more information check https://redis.io/commands/zinter
        """
        return self._zaggregate('ZINTER', None, keys, aggregate,
                                withscores=withscores)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into a new
        sorted set, ``dest``. Scores in the destination will be aggregated
        based on the ``aggregate``. This option defaults to SUM, where the
        score of an element is summed across the inputs where it exists.
        When this option is set to either MIN or MAX, the resulting set will
        contain the minimum or maximum score of an element across the inputs
        where it exists.

        For more information check https://redis.io/commands/zinterstore
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.

        For more information check https://redis.io/commands/zlexcount
        """
        return self.execute_command('ZLEXCOUNT', name, min, max)

    def zpopmax(self, name, count=None):
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmax
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return self.execute_command('ZPOPMAX', name, *args, **options)

    def zpopmin(self, name, count=None):
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmin
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return self.execute_command('ZPOPMIN', name, *args, **options)

    def zrandmember(self, key, count=None, withscores=False):
        """
        Return a random element from the sorted set value stored at key.

        ``count`` if the argument is positive, return an array of distinct
        fields. If called with a negative count, the behavior changes and
        the command is allowed to return the same field multiple times.
        In this case, the number of returned fields is the absolute value
        of the specified count.

        ``withscores`` The optional WITHSCORES modifier changes the reply so it
        includes the respective scores of the randomly selected elements from
        the sorted set.

        For more information check https://redis.io/commands/zrandmember
        """
        params = []
        if count is not None:
            params.append(count)
        if withscores:
            params.append("WITHSCORES")

        return self.execute_command("ZRANDMEMBER", key, *params)

    def bzpopmax(self, keys, timeout=0):
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmax
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BZPOPMAX', *keys)

    def bzpopmin(self, keys, timeout=0):
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmin
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BZPOPMIN', *keys)

    def _zrange(self, command, dest, name, start, end, desc=False,
                byscore=False, bylex=False, withscores=False,
                score_cast_func=float, offset=None, num=None):
        if byscore and bylex:
            raise DataError("``byscore`` and ``bylex`` can not be "
                            "specified together.")
        if (offset is not None and num is None) or \
                (num is not None and offset is None):
            raise DataError("``offset`` and ``num`` must both be specified.")
        if bylex and withscores:
            raise DataError("``withscores`` not supported in combination "
                            "with ``bylex``.")
        pieces = [command]
        if dest:
            pieces.append(dest)
        pieces.extend([name, start, end])
        if byscore:
            pieces.append('BYSCORE')
        if bylex:
            pieces.append('BYLEX')
        if desc:
            pieces.append('REV')
        if offset is not None and num is not None:
            pieces.extend(['LIMIT', offset, num])
        if withscores:
            pieces.append('WITHSCORES')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float, byscore=False, bylex=False,
               offset=None, num=None):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrange
        """
        # Need to support ``desc`` also when using old redis version
        # because it was supported in 3.5.3 (of redis-py)
        if not byscore and not bylex and (offset is None and num is None) \
                and desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)

        return self._zrange('ZRANGE', None, name, start, end, desc, byscore,
                            bylex, withscores, score_cast_func, offset, num)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrevrange
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append(b'WITHSCORES')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrangestore(self, dest, name, start, end,
                    byscore=False, bylex=False, desc=False,
                    offset=None, num=None):
        """
        Stores in ``dest`` the result of a range of values from sorted set
        ``name`` between ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrangestore
        """
        return self._zrange('ZRANGESTORE', dest, name, start, end, desc,
                            byscore, bylex, False, None, offset, num)

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information check https://redis.io/commands/zrangebylex
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYLEX', name, min, max]
        if start is not None and num is not None:
            pieces.extend([b'LIMIT', start, num])
        return self.execute_command(*pieces)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information check https://redis.io/commands/zrevrangebylex
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYLEX', name, max, min]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrangebyscore
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('WITHSCORES')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrevrangebyscore
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('WITHSCORES')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``

        For more information check https://redis.io/commands/zrank
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, *values):
        """
        Remove member ``values`` from sorted set ``name``

        For more information check https://redis.io/commands/zrem
        """
        return self.execute_command('ZREM', name, *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.

        For more information check https://redis.io/commands/zremrangebylex
        """
        return self.execute_command('ZREMRANGEBYLEX', name, min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed

        For more information check https://redis.io/commands/zremrangebyrank
        """
        return self.execute_command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.

        For more information check https://redis.io/commands/zremrangebyscore
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``

        For more information check https://redis.io/commands/zrevrank
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        """
        Return the score of element ``value`` in sorted set ``name``

        For more information check https://redis.io/commands/zscore
        """
        return self.execute_command('ZSCORE', name, value)

    def zunion(self, keys, aggregate=None, withscores=False):
        """
        Return the union of multiple sorted sets specified by ``keys``.
        ``keys`` can be provided as dictionary of keys and their weights.
        Scores will be aggregated based on the ``aggregate``, or SUM if
        none is provided.

        For more information check https://redis.io/commands/zunion
        """
        return self._zaggregate('ZUNION', None, keys, aggregate,
                                withscores=withscores)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.

        For more information check https://redis.io/commands/zunionstore
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def zmscore(self, key, members):
        """
        Returns the scores associated with the specified members
        in the sorted set stored at key.
        ``members`` should be a list of the member name.
        Return type is a list of score.
        If the member does not exist, a None will be returned
        in corresponding position.

        For more information check https://redis.io/commands/zmscore
        """
        if not members:
            raise DataError('ZMSCORE members must be a non-empty list')
        pieces = [key] + members
        return self.execute_command('ZMSCORE', *pieces)

    def _zaggregate(self, command, dest, keys, aggregate=None,
                    **options):
        pieces = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.append(len(keys))
        if isinstance(keys, dict):
            keys, weights = keys.keys(), keys.values()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(b'WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            if aggregate.upper() in ['SUM', 'MIN', 'MAX']:
                pieces.append(b'AGGREGATE')
                pieces.append(aggregate)
            else:
                raise DataError("aggregate can be sum, min or max.")
        if options.get('withscores', False):
            pieces.append(b'WITHSCORES')
        return self.execute_command(*pieces, **options)

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        """
        Adds the specified elements to the specified HyperLogLog.

        For more information check https://redis.io/commands/pfadd
        """
        return self.execute_command('PFADD', name, *values)

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).

        For more information check https://redis.io/commands/pfcount
        """
        return self.execute_command('PFCOUNT', *sources)

    def pfmerge(self, dest, *sources):
        """
        Merge N different HyperLogLogs into a single one.

        For more information check https://redis.io/commands/pfmerge
        """
        return self.execute_command('PFMERGE', dest, *sources)

    # HASH COMMANDS
    def hdel(self, name, *keys):
        """
        Delete ``keys`` from hash ``name``

        For more information check https://redis.io/commands/hdel
        """
        return self.execute_command('HDEL', name, *keys)

    def hexists(self, name, key):
        """
        Returns a boolean indicating if ``key`` exists within hash ``name``

        For more information check https://redis.io/commands/hexists
        """
        return self.execute_command('HEXISTS', name, key)

    def hget(self, name, key):
        """
        Return the value of ``key`` within the hash ``name``

        For more information check https://redis.io/commands/hget
        """
        return self.execute_command('HGET', name, key)

    def hgetall(self, name):
        """
        Return a Python dict of the hash's name/value pairs

        For more information check https://redis.io/commands/hgetall
        """
        return self.execute_command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        """
        Increment the value of ``key`` in hash ``name`` by ``amount``

        For more information check https://redis.io/commands/hincrby
        """
        return self.execute_command('HINCRBY', name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``

        For more information check https://redis.io/commands/hincrbyfloat
        """
        return self.execute_command('HINCRBYFLOAT', name, key, amount)

    def hkeys(self, name):
        """
        Return the list of keys within hash ``name``

        For more information check https://redis.io/commands/hkeys
        """
        return self.execute_command('HKEYS', name)

    def hlen(self, name):
        """
        Return the number of elements in hash ``name``

        For more information check https://redis.io/commands/hlen
        """
        return self.execute_command('HLEN', name)

    def hset(self, name, key=None, value=None, mapping=None):
        """
        Set ``key`` to ``value`` within hash ``name``,
        ``mapping`` accepts a dict of key/value pairs that will be
        added to hash ``name``.
        Returns the number of fields that were added.

        For more information check https://redis.io/commands/hset
        """
        if key is None and not mapping:
            raise DataError("'hset' with no key value pairs")
        items = []
        if key is not None:
            items.extend((key, value))
        if mapping:
            for pair in mapping.items():
                items.extend(pair)

        return self.execute_command('HSET', name, *items)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.

        For more information check https://redis.io/commands/hsetnx
        """
        return self.execute_command('HSETNX', name, key, value)

    def hmset(self, name, mapping):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.

        For more information check https://redis.io/commands/hmset
        """
        warnings.warn(
            '%s.hmset() is deprecated. Use %s.hset() instead.'
            % (self.__class__.__name__, self.__class__.__name__),
            DeprecationWarning,
            stacklevel=2,
        )
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command('HMSET', name, *items)

    def hmget(self, name, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        For more information check https://redis.io/commands/hmget
        """
        args = list_or_args(keys, args)
        return self.execute_command('HMGET', name, *args)

    def hvals(self, name):
        """
        Return the list of values within hash ``name``

        For more information check https://redis.io/commands/hvals
        """
        return self.execute_command('HVALS', name)

    def hstrlen(self, name, key):
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``

        For more information check https://redis.io/commands/hstrlen
        """
        return self.execute_command('HSTRLEN', name, key)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.

        For more information check https://redis.io/commands/publish
        """
        return self.execute_command('PUBLISH', channel, message)

    def pubsub_channels(self, pattern='*'):
        """
        Return a list of channels that have at least one subscriber

        For more information check https://redis.io/commands/pubsub-channels
        """
        return self.execute_command('PUBSUB CHANNELS', pattern)

    def pubsub_numpat(self):
        """
        Returns the number of subscriptions to patterns

        For more information check https://redis.io/commands/pubsub-numpat
        """
        return self.execute_command('PUBSUB NUMPAT')

    def pubsub_numsub(self, *args):
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``

        For more information check https://redis.io/commands/pubsub-numsub
        """
        return self.execute_command('PUBSUB NUMSUB', *args)

    def cluster(self, cluster_arg, *args):
        return self.execute_command('CLUSTER %s' % cluster_arg.upper(), *args)

    def replicaof(self, *args):
        """
        Update the replication settings of a redis replica, on the fly.
        Examples of valid arguments include:
            NO ONE (set no replication)
            host port (set to the host and port of a redis server)

        For more information check  https://redis.io/commands/replicaof
        """
        return self.execute_command('REPLICAOF', *args)

    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information check  https://redis.io/commands/eval
        """
        return self.execute_command('EVAL', script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information check  https://redis.io/commands/evalsha
        """
        return self.execute_command('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.

        For more information check  https://redis.io/commands/script-exists
        """
        return self.execute_command('SCRIPT EXISTS', *args)

    def script_debug(self, *args):
        raise NotImplementedError(
            "SCRIPT DEBUG is intentionally not implemented in the client."
        )

    def script_flush(self, sync_type=None):
        """Flush all scripts from the script cache.
        ``sync_type`` is by default SYNC (synchronous) but it can also be
                      ASYNC.
        For more information check  https://redis.io/commands/script-flush
        """

        # Redis pre 6 had no sync_type.
        if sync_type not in ["SYNC", "ASYNC", None]:
            raise DataError("SCRIPT FLUSH defaults to SYNC in redis > 6.2, or "
                            "accepts SYNC/ASYNC. For older versions, "
                            "of redis leave as None.")
        if sync_type is None:
            pieces = []
        else:
            pieces = [sync_type]
        return self.execute_command('SCRIPT FLUSH', *pieces)

    def script_kill(self):
        """
        Kill the currently executing Lua script

        For more information check https://redis.io/commands/script-kill
        """
        return self.execute_command('SCRIPT KILL')

    def script_load(self, script):
        """
        Load a Lua ``script`` into the script cache. Returns the SHA.

        For more information check https://redis.io/commands/script-load
        """
        return self.execute_command('SCRIPT LOAD', script)

    def register_script(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    # GEO COMMANDS
    def geoadd(self, name, values, nx=False, xx=False, ch=False):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.

        Note: You can use ZREM to remove elements.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        For more information check https://redis.io/commands/geoadd
        """
        if nx and xx:
            raise DataError("GEOADD allows either 'nx' or 'xx', not both")
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name"
                            " values")
        pieces = [name]
        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        if ch:
            pieces.append('CH')
        pieces.extend(values)
        return self.execute_command('GEOADD', *pieces)

    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.

        For more information check https://redis.io/commands/geodist
        """
        pieces = [name, place1, place2]
        if unit and unit not in ('m', 'km', 'mi', 'ft'):
            raise DataError("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.execute_command('GEODIST', *pieces)

    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.

        For more information check https://redis.io/commands/geohash
        """
        return self.execute_command('GEOHASH', name, *values)

    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.

        For more information check https://redis.io/commands/geopos
        """
        return self.execute_command('GEOPOS', name, *values)

    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None, any=False):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.

        For more information check https://redis.io/commands/georadius
        """
        return self._georadiusgeneric('GEORADIUS',
                                      name, longitude, latitude, radius,
                                      unit=unit, withdist=withdist,
                                      withcoord=withcoord, withhash=withhash,
                                      count=count, sort=sort, store=store,
                                      store_dist=store_dist, any=any)

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None,
                          any=False):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.

        For more information check https://redis.io/commands/georadiusbymember
        """
        return self._georadiusgeneric('GEORADIUSBYMEMBER',
                                      name, member, radius, unit=unit,
                                      withdist=withdist, withcoord=withcoord,
                                      withhash=withhash, count=count,
                                      sort=sort, store=store,
                                      store_dist=store_dist, any=any)

    def _georadiusgeneric(self, command, *args, **kwargs):
        pieces = list(args)
        if kwargs['unit'] and kwargs['unit'] not in ('m', 'km', 'mi', 'ft'):
            raise DataError("GEORADIUS invalid unit")
        elif kwargs['unit']:
            pieces.append(kwargs['unit'])
        else:
            pieces.append('m',)

        if kwargs['any'] and kwargs['count'] is None:
            raise DataError("``any`` can't be provided without ``count``")

        for arg_name, byte_repr in (
                ('withdist', 'WITHDIST'),
                ('withcoord', 'WITHCOORD'),
                ('withhash', 'WITHHASH')):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        if kwargs['count'] is not None:
            pieces.extend(['COUNT', kwargs['count']])
            if kwargs['any']:
                pieces.append('ANY')

        if kwargs['sort']:
            if kwargs['sort'] == 'ASC':
                pieces.append('ASC')
            elif kwargs['sort'] == 'DESC':
                pieces.append('DESC')
            else:
                raise DataError("GEORADIUS invalid sort")

        if kwargs['store'] and kwargs['store_dist']:
            raise DataError("GEORADIUS store and store_dist cant be set"
                            " together")

        if kwargs['store']:
            pieces.extend([b'STORE', kwargs['store']])

        if kwargs['store_dist']:
            pieces.extend([b'STOREDIST', kwargs['store_dist']])

        return self.execute_command(command, *pieces, **kwargs)

    def geosearch(self, name, member=None, longitude=None, latitude=None,
                  unit='m', radius=None, width=None, height=None, sort=None,
                  count=None, any=False, withcoord=False,
                  withdist=False, withhash=False):
        """
        Return the members of specified key identified by the
        ``name`` argument, which are within the borders of the
        area specified by a given shape. This command extends the
        GEORADIUS command, so in addition to searching within circular
        areas, it supports searching within rectangular areas.
        This command should be used in place of the deprecated
        GEORADIUS and GEORADIUSBYMEMBER commands.
        ``member`` Use the position of the given existing
         member in the sorted set. Can't be given with ``longitude``
         and ``latitude``.
        ``longitude`` and ``latitude`` Use the position given by
        this coordinates. Can't be given with ``member``
        ``radius`` Similar to GEORADIUS, search inside circular
        area according the given radius. Can't be given with
        ``height`` and ``width``.
        ``height`` and ``width`` Search inside an axis-aligned
        rectangle, determined by the given height and width.
        Can't be given with ``radius``
        ``unit`` must be one of the following : m, km, mi, ft.
        `m` for meters (the default value), `km` for kilometers,
        `mi` for miles and `ft` for feet.
        ``sort`` indicates to return the places in a sorted way,
        ASC for nearest to farest and DESC for farest to nearest.
        ``count`` limit the results to the first count matching items.
        ``any`` is set to True, the command will return as soon as
        enough matches are found. Can't be provided without ``count``
        ``withdist`` indicates to return the distances of each place.
        ``withcoord`` indicates to return the latitude and longitude of
        each place.
        ``withhash`` indicates to return the geohash string of each place.

        For more information check https://redis.io/commands/geosearch
        """

        return self._geosearchgeneric('GEOSEARCH',
                                      name, member=member, longitude=longitude,
                                      latitude=latitude, unit=unit,
                                      radius=radius, width=width,
                                      height=height, sort=sort, count=count,
                                      any=any, withcoord=withcoord,
                                      withdist=withdist, withhash=withhash,
                                      store=None, store_dist=None)

    def geosearchstore(self, dest, name, member=None, longitude=None,
                       latitude=None, unit='m', radius=None, width=None,
                       height=None, sort=None, count=None, any=False,
                       storedist=False):
        """
        This command is like GEOSEARCH, but stores the result in
        ``dest``. By default, it stores the results in the destination
        sorted set with their geospatial information.
        if ``store_dist`` set to True, the command will stores the
        items in a sorted set populated with their distance from the
        center of the circle or box, as a floating-point number.

        For more information check https://redis.io/commands/geosearchstore
        """
        return self._geosearchgeneric('GEOSEARCHSTORE',
                                      dest, name, member=member,
                                      longitude=longitude, latitude=latitude,
                                      unit=unit, radius=radius, width=width,
                                      height=height, sort=sort, count=count,
                                      any=any, withcoord=None,
                                      withdist=None, withhash=None,
                                      store=None, store_dist=storedist)

    def _geosearchgeneric(self, command, *args, **kwargs):
        pieces = list(args)

        # FROMMEMBER or FROMLONLAT
        if kwargs['member'] is None:
            if kwargs['longitude'] is None or kwargs['latitude'] is None:
                raise DataError("GEOSEARCH must have member or"
                                " longitude and latitude")
        if kwargs['member']:
            if kwargs['longitude'] or kwargs['latitude']:
                raise DataError("GEOSEARCH member and longitude or latitude"
                                " cant be set together")
            pieces.extend([b'FROMMEMBER', kwargs['member']])
        if kwargs['longitude'] and kwargs['latitude']:
            pieces.extend([b'FROMLONLAT',
                           kwargs['longitude'], kwargs['latitude']])

        # BYRADIUS or BYBOX
        if kwargs['radius'] is None:
            if kwargs['width'] is None or kwargs['height'] is None:
                raise DataError("GEOSEARCH must have radius or"
                                " width and height")
        if kwargs['unit'] is None:
            raise DataError("GEOSEARCH must have unit")
        if kwargs['unit'].lower() not in ('m', 'km', 'mi', 'ft'):
            raise DataError("GEOSEARCH invalid unit")
        if kwargs['radius']:
            if kwargs['width'] or kwargs['height']:
                raise DataError("GEOSEARCH radius and width or height"
                                " cant be set together")
            pieces.extend([b'BYRADIUS', kwargs['radius'], kwargs['unit']])
        if kwargs['width'] and kwargs['height']:
            pieces.extend([b'BYBOX',
                           kwargs['width'], kwargs['height'], kwargs['unit']])

        # sort
        if kwargs['sort']:
            if kwargs['sort'].upper() == 'ASC':
                pieces.append(b'ASC')
            elif kwargs['sort'].upper() == 'DESC':
                pieces.append(b'DESC')
            else:
                raise DataError("GEOSEARCH invalid sort")

        # count any
        if kwargs['count']:
            pieces.extend([b'COUNT', kwargs['count']])
            if kwargs['any']:
                pieces.append(b'ANY')
        elif kwargs['any']:
            raise DataError("GEOSEARCH ``any`` can't be provided "
                            "without count")

        # other properties
        for arg_name, byte_repr in (
                ('withdist', b'WITHDIST'),
                ('withcoord', b'WITHCOORD'),
                ('withhash', b'WITHHASH'),
                ('store_dist', b'STOREDIST')):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        return self.execute_command(command, *pieces, **kwargs)

    # MODULE COMMANDS
    def module_load(self, path, *args):
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.

        For more information check https://redis.io/commands/module-load
        """
        return self.execute_command('MODULE LOAD', path, *args)

    def module_unload(self, name):
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.

        For more information check https://redis.io/commands/module-unload
        """
        return self.execute_command('MODULE UNLOAD', name)

    def module_list(self):
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.

        For more information check https://redis.io/commands/module-list
        """
        return self.execute_command('MODULE LIST')

    def command_info(self):
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    def command_count(self):
        return self.execute_command('COMMAND COUNT')

    def command(self):
        return self.execute_command('COMMAND')


class Script:
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, str):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            encoder = registered_client.connection_pool.get_encoder()
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        from redis.client import Pipeline
        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)


class BitFieldOperation:
    """
    Command builder for BITFIELD commands.
    """
    def __init__(self, client, key, default_overflow=None):
        self.client = client
        self.key = key
        self._default_overflow = default_overflow
        self.reset()

    def reset(self):
        """
        Reset the state of the instance to when it was constructed
        """
        self.operations = []
        self._last_overflow = 'WRAP'
        self.overflow(self._default_overflow or self._last_overflow)

    def overflow(self, overflow):
        """
        Update the overflow algorithm of successive INCRBY operations
        :param overflow: Overflow algorithm, one of WRAP, SAT, FAIL. See the
            Redis docs for descriptions of these algorithmsself.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        overflow = overflow.upper()
        if overflow != self._last_overflow:
            self._last_overflow = overflow
            self.operations.append(('OVERFLOW', overflow))
        return self

    def incrby(self, fmt, offset, increment, overflow=None):
        """
        Increment a bitfield by a given amount.
        :param fmt: format-string for the bitfield being updated, e.g. 'u8'
            for an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, but other
            acceptable values are SAT and FAIL. See the Redis docs for
            descriptions of these algorithms.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        if overflow is not None:
            self.overflow(overflow)

        self.operations.append(('INCRBY', fmt, offset, increment))
        return self

    def get(self, fmt, offset):
        """
        Get the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(('GET', fmt, offset))
        return self

    def set(self, fmt, offset, value):
        """
        Set the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int value: value to set at the given position.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(('SET', fmt, offset, value))
        return self

    @property
    def command(self):
        cmd = ['BITFIELD', self.key]
        for ops in self.operations:
            cmd.extend(ops)
        return cmd

    def execute(self):
        """
        Execute the operation(s) in a single BITFIELD command. The return value
        is a list of values corresponding to each operation. If the client
        used to create this instance was a pipeline, the list of values
        will be present within the pipeline's execute.
        """
        command = self.command
        self.reset()
        return self.client.execute_command(*command)
