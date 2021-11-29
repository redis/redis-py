from enum import Enum


class IndexType(Enum):
    """Enum of the currently supported index types."""

    HASH = 1
    JSON = 2


class IndexDefinition:
    """IndexDefinition is used to define a index definition for automatic
    indexing on Hash or Json update."""

    def __init__(
        self,
        prefix=[],
        filter=None,
        language_field=None,
        language=None,
        score_field=None,
        score=1.0,
        payload_field=None,
        index_type=None,
    ):
        self.args = []
        self._appendIndexType(index_type)
        self._appendPrefix(prefix)
        self._appendFilter(filter)
        self._appendLanguage(language_field, language)
        self._appendScore(score_field, score)
        self._appendPayload(payload_field)

    def _appendIndexType(self, index_type):
        """Append `ON HASH` or `ON JSON` according to the enum."""
        if index_type is IndexType.HASH:
            self.args.extend(["ON", "HASH"])
        elif index_type is IndexType.JSON:
            self.args.extend(["ON", "JSON"])
        elif index_type is not None:
            raise RuntimeError(f"index_type must be one of {list(IndexType)}")

    def _appendPrefix(self, prefix):
        """Append PREFIX."""
        if len(prefix) > 0:
            self.args.append("PREFIX")
            self.args.append(len(prefix))
            for p in prefix:
                self.args.append(p)

    def _appendFilter(self, filter):
        """Append FILTER."""
        if filter is not None:
            self.args.append("FILTER")
            self.args.append(filter)

    def _appendLanguage(self, language_field, language):
        """Append LANGUAGE_FIELD and LANGUAGE."""
        if language_field is not None:
            self.args.append("LANGUAGE_FIELD")
            self.args.append(language_field)
        if language is not None:
            self.args.append("LANGUAGE")
            self.args.append(language)

    def _appendScore(self, score_field, score):
        """Append SCORE_FIELD and SCORE."""
        if score_field is not None:
            self.args.append("SCORE_FIELD")
            self.args.append(score_field)
        if score is not None:
            self.args.append("SCORE")
            self.args.append(score)

    def _appendPayload(self, payload_field):
        """Append PAYLOAD_FIELD."""
        if payload_field is not None:
            self.args.append("PAYLOAD_FIELD")
            self.args.append(payload_field)
