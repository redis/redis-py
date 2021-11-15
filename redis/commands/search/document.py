class Document(object):
    """
    Represents a single document in a result set
    """

    def __init__(self, id, payload=None, **fields):
        self.id = id
        self.payload = payload
        for k, v in fields.items():
            setattr(self, k, v)

    def __repr__(self):
        return "Document %s" % self.__dict__
