def str_path(p):
    """Return the string representation of a path if it is of class Path."""
    if isinstance(p, Path):
        return p.strPath
    else:
        return p


class Path(object):
    """This class represents a path in a JSON value."""

    strPath = ""

    @staticmethod
    def rootPath():
        """Return the root path's string representation."""
        return "."

    def __init__(self, path):
        """Make a new path based on the string representation in `path`."""
        self.strPath = path
