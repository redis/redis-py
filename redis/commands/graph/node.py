from ..helpers import quote_string


class Node:
    """
    A node within the graph.
    """

    def __init__(self, node_id=None, alias=None, label=None, properties=None):
        """
        Create a new node.
        """
        self.id = node_id
        self.alias = alias
        self.label = label
        self.properties = properties or {}

    def toString(self):
        res = ""
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += "{" + props + "}"

        return res

    def __str__(self):
        res = "("
        if self.alias:
            res += self.alias
        if self.label:
            res += ":" + self.label
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += "{" + props + "}"
        res += ")"

        return res

    def __eq__(self, rhs):
        # Quick positive check, if both IDs are set.
        if self.id is not None and rhs.id is not None and self.id != rhs.id:
            return False

        # Label should match.
        if self.label != rhs.label:
            return False

        # Quick check for number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True
