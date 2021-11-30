from ..helpers import quote_string, random_string, stringify_param_value
from .commands import GraphCommands
from .edge import Edge  # noqa
from .node import Node  # noqa
from .path import Path  # noqa


class Graph(GraphCommands):
    """
    Graph, collection of nodes and edges.
    """

    def __init__(self, client, name=random_string()):
        """
        Create a new graph.
        """
        self.NAME = name  # Graph key
        self.client = client
        self.execute_command = client.execute_command

        self.nodes = {}
        self.edges = []
        self._labels = []  # List of node labels.
        self._properties = []  # List of properties.
        self._relationshipTypes = []  # List of relation types.
        self.version = 0  # Graph version

    @property
    def name(self):
        return self.NAME

    def _clear_schema(self):
        self._labels = []
        self._properties = []
        self._relationshipTypes = []

    def _refresh_schema(self):
        self._clear_schema()
        self._refresh_labels()
        self._refresh_relations()
        self._refresh_attributes()

    def _refresh_labels(self):
        lbls = self.labels()

        # Unpack data.
        self._labels = [None] * len(lbls)
        for i, l in enumerate(lbls):
            self._labels[i] = l[0]

    def _refresh_relations(self):
        rels = self.relationshipTypes()

        # Unpack data.
        self._relationshipTypes = [None] * len(rels)
        for i, r in enumerate(rels):
            self._relationshipTypes[i] = r[0]

    def _refresh_attributes(self):
        props = self.propertyKeys()

        # Unpack data.
        self._properties = [None] * len(props)
        for i, p in enumerate(props):
            self._properties[i] = p[0]

    def get_label(self, idx):
        """
        Returns a label by it's index

        Args:

        idx:
            The index of the label
        """
        try:
            label = self._labels[idx]
        except IndexError:
            # Refresh labels.
            self._refresh_labels()
            label = self._labels[idx]
        return label

    def get_relation(self, idx):
        """
        Returns a relationship type by it's index

        Args:

        idx:
            The index of the relation
        """
        try:
            relationship_type = self._relationshipTypes[idx]
        except IndexError:
            # Refresh relationship types.
            self._refresh_relations()
            relationship_type = self._relationshipTypes[idx]
        return relationship_type

    def get_property(self, idx):
        """
        Returns a property by it's index

        Args:

        idx:
            The index of the property
        """
        try:
            propertie = self._properties[idx]
        except IndexError:
            # Refresh properties.
            self._refresh_attributes()
            propertie = self._properties[idx]
        return propertie

    def add_node(self, node):
        """
        Adds a node to the graph.
        """
        if node.alias is None:
            node.alias = random_string()
        self.nodes[node.alias] = node

    def add_edge(self, edge):
        """
        Adds an edge to the graph.
        """
        if not (self.nodes[edge.src_node.alias] and self.nodes[edge.dest_node.alias]):
            raise AssertionError("Both edge's end must be in the graph")

        self.edges.append(edge)

    def _build_params_header(self, params):
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

    # Procedures.
    def call_procedure(self, procedure, *args, read_only=False, **kwagrs):
        args = [quote_string(arg) for arg in args]
        q = f"CALL {procedure}({','.join(args)})"

        y = kwagrs.get("y", None)
        if y:
            q += f" YIELD {','.join(y)}"

        return self.query(q, read_only=read_only)

    def labels(self):
        return self.call_procedure("db.labels", read_only=True).result_set

    def relationshipTypes(self):
        return self.call_procedure("db.relationshipTypes", read_only=True).result_set

    def propertyKeys(self):
        return self.call_procedure("db.propertyKeys", read_only=True).result_set
