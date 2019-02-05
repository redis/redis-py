import errno
import select
from redis.exceptions import RedisError


_DEFAULT_SELECTOR = None


class BaseSelector(object):
    """
    Base class for all Selectors
    """
    def __init__(self, sock):
        self.sock = sock

    def can_read(self, timeout=0):
        """
        Return True if data is ready to be read from the socket,
        otherwise False.

        This doesn't guarentee that the socket is still connected, just that
        there is data to read.

        Automatically retries EINTR errors based on PEP 475.
        """
        while True:
            try:
                return self.check_can_read(timeout)
            except (select.error, IOError) as ex:
                if self.errno_from_exception(ex) == errno.EINTR:
                    continue
                return False

    def is_ready_for_command(self, timeout=0):
        """
        Return True if the socket is ready to send a command,
        otherwise False.

        Automatically retries EINTR errors based on PEP 475.
        """
        while True:
            try:
                return self.check_is_ready_for_command(timeout)
            except (select.error, IOError) as ex:
                if self.errno_from_exception(ex) == errno.EINTR:
                    continue
                return False

    def check_can_read(self, timeout):
        """
        Perform the can_read check. Subclasses should implement this.
        """
        raise NotImplementedError

    def check_is_ready_for_command(self, timeout):
        """
        Perform the is_ready_for_command check. Subclasses should
        implement this.
        """
        raise NotImplementedError

    def close(self):
        """
        Close the selector.
        """
        self.sock = None

    def errno_from_exception(self, ex):
        """
        Get the error number from an exception
        """
        if hasattr(ex, 'errno'):
            return ex.errno
        elif ex.args:
            return ex.args[0]
        else:
            return None


if hasattr(select, 'select'):
    class SelectSelector(BaseSelector):
        """
        A select-based selector that should work on most platforms.

        This is the worst poll strategy and should only be used if no other
        option is available.
        """
        def check_can_read(self, timeout):
            """
            Return True if data is ready to be read from the socket,
            otherwise False.

            This doesn't guarentee that the socket is still connected, just
            that there is data to read.
            """
            return bool(select.select([self.sock], [], [], timeout)[0])

        def check_is_ready_for_command(self, timeout):
            """
            Return True if the socket is ready to send a command,
            otherwise False.
            """
            r, w, e = select.select([self.sock], [self.sock], [self.sock],
                                    timeout)
            return bool(w and not r and not e)


if hasattr(select, 'poll'):
    class PollSelector(BaseSelector):
        """
        A poll-based selector that should work on (almost?) all versions
        of Unix
        """
        _EVENT_MASK = (select.POLLIN | select.POLLPRI | select.POLLOUT |
                       select.POLLERR | select.POLLHUP)
        _READ_MASK = select.POLLIN | select.POLLPRI
        _WRITE_MASK = select.POLLOUT

        def __init__(self, sock):
            super().__init__(sock)
            self.poller = select.poll()
            self.poller.register(sock, self._EVENT_MASK)

        def close(self):
            """
            Close the selector.
            """
            try:
                self.poller.unregister(self.sock)
            except (KeyError, ValueError):
                # KeyError is raised if somehow the socket was not registered
                # ValueError is raised if the socket's file descriptor is
                #   negative.
                # In either case, we can't do anything better than to remove
                # the reference to the poller.
                pass
            self.poller = None
            self.sock = None

        def check_can_read(self, timeout=0):
            """
            Return True if data is ready to be read from the socket,
            otherwise False.

            This doesn't guarentee that the socket is still connected, just
            that there is data to read.
            """
            events = self.poller.poll(0)
            return bool(events and events[0][1] & self._READ_MASK)

        def check_is_ready_for_command(self, timeout=0):
            """
            Return True if the socket is ready to send a command,
            otherwise False
            """
            events = self.poller.poll(0)
            return bool(events and events[0][1] == self._WRITE_MASK)


def has_selector(selector):
    "Determine if the current platform has the selector available"
    try:
        if selector == 'poll':
            # the select module offers the poll selector even if the platform
            # doesn't support it. Attempt to poll for nothing to make sure
            # poll is available
            p = select.poll()
            p.poll(0)
        else:
            # the other selectors will fail when instantiated
            getattr(select, selector)().close()
        return True
    except (OSError, AttributeError):
        return False


def DefaultSelector(sock):
    "Return the best selector for the platform"
    global _DEFAULT_SELECTOR
    if _DEFAULT_SELECTOR is None:
        if has_selector('poll'):
            _DEFAULT_SELECTOR = PollSelector
        elif hasattr(select, 'select'):
            _DEFAULT_SELECTOR = SelectSelector
        else:
            raise RedisError('Platform does not support any selectors')
    return _DEFAULT_SELECTOR(sock)
