class Field:

    NUMERIC = "NUMERIC"
    TEXT = "TEXT"
    WEIGHT = "WEIGHT"
    GEO = "GEO"
    TAG = "TAG"
    SORTABLE = "SORTABLE"
    NOINDEX = "NOINDEX"
    AS = "AS"

    def __init__(self, name, args=[], sortable=False, no_index=False, as_name=None):
        self.name = name
        self.args = args
        self.args_suffix = list()
        self.as_name = as_name

        if sortable:
            self.args_suffix.append(Field.SORTABLE)
        if no_index:
            self.args_suffix.append(Field.NOINDEX)

        if no_index and not sortable:
            raise ValueError("Non-Sortable non-Indexable fields are ignored")

    def append_arg(self, value):
        self.args.append(value)

    def redis_args(self):
        args = [self.name]
        if self.as_name:
            args += [self.AS, self.as_name]
        args += self.args
        args += self.args_suffix
        return args


class TextField(Field):
    """
    TextField is used to define a text field in a schema definition
    """

    NOSTEM = "NOSTEM"
    PHONETIC = "PHONETIC"

    def __init__(
        self, name, weight=1.0, no_stem=False, phonetic_matcher=None, **kwargs
    ):
        Field.__init__(self, name, args=[Field.TEXT, Field.WEIGHT, weight], **kwargs)

        if no_stem:
            Field.append_arg(self, self.NOSTEM)
        if phonetic_matcher and phonetic_matcher in [
            "dm:en",
            "dm:fr",
            "dm:pt",
            "dm:es",
        ]:
            Field.append_arg(self, self.PHONETIC)
            Field.append_arg(self, phonetic_matcher)


class NumericField(Field):
    """
    NumericField is used to define a numeric field in a schema definition
    """

    def __init__(self, name, **kwargs):
        Field.__init__(self, name, args=[Field.NUMERIC], **kwargs)


class GeoField(Field):
    """
    GeoField is used to define a geo-indexing field in a schema definition
    """

    def __init__(self, name, **kwargs):
        Field.__init__(self, name, args=[Field.GEO], **kwargs)


class TagField(Field):
    """
    TagField is a tag-indexing field with simpler compression and tokenization.
    See http://redisearch.io/Tags/
    """

    SEPARATOR = "SEPARATOR"

    def __init__(self, name, separator=",", **kwargs):
        Field.__init__(
            self, name, args=[Field.TAG, self.SEPARATOR, separator], **kwargs
        )
