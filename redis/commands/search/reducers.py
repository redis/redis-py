from typing import Union

from .aggregation import Asc, Desc, Reducer, SortDirection


def _ensure_at_prefix(name: str) -> str:
    """Return ``name`` with a single leading ``@`` prefix."""
    return name if name.startswith("@") else "@" + name


class FieldOnlyReducer(Reducer):
    """See https://redis.io/docs/interact/search-and-query/search/aggregations/"""

    def __init__(self, field: str) -> None:
        super().__init__(field)
        self._field = field


class count(Reducer):
    """
    Counts the number of results in the group
    """

    NAME = "COUNT"

    def __init__(self) -> None:
        super().__init__()


class sum(FieldOnlyReducer):
    """
    Calculates the sum of all the values in the given fields within the group
    """

    NAME = "SUM"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class min(FieldOnlyReducer):
    """
    Calculates the smallest value in the given field within the group
    """

    NAME = "MIN"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class max(FieldOnlyReducer):
    """
    Calculates the largest value in the given field within the group
    """

    NAME = "MAX"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class avg(FieldOnlyReducer):
    """
    Calculates the mean value in the given field within the group
    """

    NAME = "AVG"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class tolist(FieldOnlyReducer):
    """
    Returns all the matched properties in a list
    """

    NAME = "TOLIST"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class count_distinct(FieldOnlyReducer):
    """
    Calculate the number of distinct values contained in all the results in
    the group for the given field
    """

    NAME = "COUNT_DISTINCT"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class count_distinctish(FieldOnlyReducer):
    """
    Calculate the number of distinct values contained in all the results in the
    group for the given field. This uses a faster algorithm than
    `count_distinct` but is less accurate
    """

    NAME = "COUNT_DISTINCTISH"


class quantile(Reducer):
    """
    Return the value for the nth percentile within the range of values for the
    field within the group.
    """

    NAME = "QUANTILE"

    def __init__(self, field: str, pct: float) -> None:
        super().__init__(field, str(pct))
        self._field = field


class stddev(FieldOnlyReducer):
    """
    Return the standard deviation for the values within the group
    """

    NAME = "STDDEV"

    def __init__(self, field: str) -> None:
        super().__init__(field)


class first_value(Reducer):
    """
    Selects the first value within the group according to sorting parameters
    """

    NAME = "FIRST_VALUE"

    def __init__(self, field: str, *byfields: Union[Asc, Desc]) -> None:
        """
        Selects the first value of the given field within the group.

        ### Parameter

        - **field**: Source field used for the value
        - **byfields**: How to sort the results. This can be either the
            *class* of `aggregation.Asc` or `aggregation.Desc` in which
            case the field `field` is also used as the sort input.

            `byfields` can also be one or more *instances* of `Asc` or `Desc`
            indicating the sort order for these fields
        """

        fieldstrs = []
        if (
            len(byfields) == 1
            and isinstance(byfields[0], type)
            and issubclass(byfields[0], SortDirection)
        ):
            byfields = [byfields[0](field)]

        for f in byfields:
            fieldstrs += [f.field, f.DIRSTRING]

        args = [field]
        if fieldstrs:
            args += ["BY"] + fieldstrs
        super().__init__(*args)
        self._field = field


class random_sample(Reducer):
    """
    Returns a random sample of items from the dataset, from the given property
    """

    NAME = "RANDOM_SAMPLE"

    def __init__(self, field: str, size: int) -> None:
        """
        ### Parameter

        **field**: Field to sample from
        **size**: Return this many items (can be less)
        """
        args = [field, str(size)]
        super().__init__(*args)
        self._field = field


class collect(Reducer):
    """
    Gathers the rows of each ``GROUPBY`` group, projects a chosen set of
    fields from every row, optionally deduplicates, sorts, and limits them,
    and returns them as an array of per-entry maps under the reducer alias.

    ``COLLECT`` is a preview feature gated behind
    ``search-enable-unstable-features``. See
    `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`_.
    """

    NAME = "COLLECT"

    def __init__(
        self,
        fields: Union[str, list[str]] = "*",
        distinct: bool = False,
        sort_by: Union[str, Asc, Desc, list[Union[str, Asc, Desc]], None] = None,
        limit: Union[tuple[int, int], None] = None,
    ) -> None:
        """
        ### Parameters

        - **fields**: The fields to project from each collected row. Either the
            literal ``"*"`` (project every field present in the pipeline at this
            stage) or a field name / list of field names. Names are normalized
            to a single leading ``@`` on the wire; output map keys are the bare
            names.
        - **distinct**: When ``True``, emit ``DISTINCT`` to deduplicate entries
            with identical projected fields. Forward-compatible: this option is
            not yet implemented by the server and currently produces a server
            error when sent.
        - **sort_by**: One field name, an ``Asc``/``Desc`` instance, or a list
            of them, used to order the collected entries within each group.
            Bare names default to ascending order.
        - **limit**: An ``(offset, count)`` pair. Returns at most ``count``
            entries per group after skipping ``offset``. With ``sort_by`` this
            acts as a top-N selection.
        """
        args: list[str] = []

        # FIELDS (required)
        if fields == "*":
            args += ["FIELDS", "*"]
        else:
            if isinstance(fields, str):
                if not fields.strip():
                    raise ValueError(
                        "collect fields must be '*' or a non-empty list of names"
                    )
                names = [fields]
            else:
                names = list(fields)
                if not names:
                    raise ValueError(
                        "collect fields must be '*' or a non-empty list of names"
                    )
            names = [_ensure_at_prefix(n) for n in names]
            args += ["FIELDS", str(len(names))] + names

        # DISTINCT (optional)
        if distinct:
            args += ["DISTINCT"]

        # SORTBY (optional)
        if sort_by is not None:
            sort_fields = (
                [sort_by] if isinstance(sort_by, (str, Asc, Desc)) else sort_by
            )
            sort_args: list[str] = []
            for f in sort_fields:
                if isinstance(f, (Asc, Desc)):
                    sort_args += [_ensure_at_prefix(f.field), f.DIRSTRING]
                else:
                    sort_args += [_ensure_at_prefix(f)]
            if not sort_args:
                raise ValueError("collect sort_by must contain at least one field")
            args += ["SORTBY", str(len(sort_args))] + sort_args

        # LIMIT (optional)
        if limit is not None:
            offset, count = limit
            args += ["LIMIT", str(offset), str(count)]

        super().__init__(*args)
