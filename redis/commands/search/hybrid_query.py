from typing import List, Literal, Optional, Union

try:
    from typing import Self  # Py 3.11+
except ImportError:
    from typing_extensions import Self

from redis.commands.search.aggregation import Limit, Reducer
from redis.commands.search.query import Filter, SortbyField


class HybridSearchQuery:
    def __init__(
        self,
        query_string: str,
        scorer: Optional[str] = None,
        yield_score_as: Optional[str] = None,
    ) -> None:
        self._query_string = query_string
        self._scorer = scorer
        self._yield_score_as = yield_score_as

    def query_string(self) -> str:
        """Return the query string of this query only."""
        return self._query_string

    def scorer(self, scorer: str) -> "HybridSearchQuery":
        """
        Scoring parameters for text search
        """
        self._scorer = scorer
        return self

    def get_args(self) -> List[str]:
        args = ["SEARCH", self._query_string]
        if self._scorer:
            scorer_pieces = self._scorer.split(" ")
            if len(scorer_pieces) > 1:
                # len(scorer_pieces) + 1 -- one of the pairs contains just one value
                # which is the scorer name
                args.extend(("SCORER", len(scorer_pieces) + 1, *scorer_pieces))
            else:
                args.extend(("SCORER", *scorer_pieces))
        return args


class HybridVsimQuery:
    def __init__(
        self,
        vector_field_name: str,
        vector_data: Union[bytes, str],
        search_method_params: Optional[str] = None,
        filter: Optional["Filter"] = None,
    ) -> None:
        self._vector_field = vector_field_name
        self._vector_data = vector_data
        self._vsim_method_params = search_method_params
        self._filter = filter

    def vector_field(self) -> str:
        """Return the vector field of this query only."""
        return self._vector_field

    def vector_data(self) -> Union[bytes, str]:
        """Return the vector data of this query only."""
        return self._vector_data

    def vsim_method_params(
        self,
        method: Literal["KNN", "RANGE"],
        **kwargs,
    ) -> "HybridVsimQuery":
        """
        Add search method parameters to the query.
        search_method_params: Vector search parameters (KNN/RANGE)
        """
        vsim_method_params = [method]
        if kwargs:
            vsim_method_params.append(len(kwargs.items()) * 2)
            for key, value in kwargs.items():
                vsim_method_params.extend((key, value))
        self._vsim_method_params = vsim_method_params
        return self

    def filter(self, flt: "HybridFilter") -> "HybridVsimQuery":
        """
        Add a numeric or string filter to the query.
        **Currently, only one of each filter is supported by the engine**

        - **flt**: A NumericFilter or GeoFilter object, used on a
        corresponding field
        """
        self._filter = flt
        return self

    def get_args(self) -> List[str]:
        args = ["VSIM", self._vector_field, self._vector_data]
        if self._vsim_method_params:
            args.extend(self._vsim_method_params)
        if self._filter:
            args.extend(self._filter.args)

        return args


class HybridQuery:
    def __init__(
        self,
        search_query: HybridSearchQuery,
        vector_similarity_query: HybridVsimQuery,
    ) -> None:
        self._search_query = search_query
        self._vector_similarity_query = vector_similarity_query

    def get_args(self) -> List[str]:
        args = []
        args.extend(self._search_query.get_args())
        args.extend(self._vector_similarity_query.get_args())
        return args


class HybridPostProcessingConfig:
    def __init__(self) -> None:
        self._combine = []
        self._load_fields = []
        self._groupby = []
        self._apply = []
        self._sortby_fields = []
        self._filter = None
        self._limit = None

    def combine(
        self,
        method: Literal["RRF", "LINEAR"],
        yield_score_as: Optional[str] = None,
        **kwargs,
    ) -> Self:
        """
        Add combine parameters to the query.

        ### Parameters
        - **method**: The combine method to use - RRF, LINEAR.
        """
        self._combine = [method]

        self._combine.append(len(kwargs) * 2)

        for key, value in kwargs.items():
            self._combine.extend([key, value])

        if yield_score_as:
            self._combine.extend(["YIELD_SCORE_AS", yield_score_as])
        return self

    def load(self, *fields: str) -> Self:
        """
        Add load parameters to the query.
        """
        self._load_fields = fields
        return self

    def group_by(
        self, fields: List[str], *reducers: Union[Reducer, List[Reducer]]
    ) -> Self:
        """
        Specify by which fields to group the aggregation.

        ### Parameters

        - **fields**: Fields to group by. This can either be a single string,
            or a list of strings. both cases, the field should be specified as
            `@field`.
        - **reducers**: One or more reducers. Reducers may be found in the
            `aggregation` module.
        """

        fields = [fields] if isinstance(fields, str) else fields
        reducers = [reducers] if isinstance(reducers, Reducer) else reducers

        ret = ["GROUPBY", str(len(fields)), *fields]
        for reducer in reducers:
            ret += ["REDUCE", reducer.NAME, str(len(reducer.args))]
            ret.extend(reducer.args)
            if reducer._alias is not None:
                ret += ["AS", reducer._alias]

        self._groupby.extend(ret)
        return self

    def apply(self, **kwexpr) -> Self:
        """
        Specify one or more projection expressions to add to each result

        ### Parameters

        - **kwexpr**: One or more key-value pairs for a projection. The key is
            the alias for the projection, and the value is the projection
            expression itself, for example `apply(square_root="sqrt(@foo)")`
        """
        for alias, expr in kwexpr.items():
            ret = ["APPLY", expr]
            if alias is not None:
                ret += ["AS", alias]
            self._apply.extend(ret)

        return self

    def sort_by(self, *sortby: "SortbyField") -> Self:
        """
        Add sortby parameters to the query.
        """
        self._sortby_fields = [*sortby]
        return self

    def filter(self, filter: "HybridFilter") -> Self:
        """
        Add a numeric or string filter to the query.
        **Currently, only one of each filter is supported by the engine**

        - **flt**: A NumericFilter or GeoFilter object, used on a
        corresponding field
        """
        self._filter = filter
        return self

    def limit(self, offset: int, num: int) -> Self:
        """
        Add limit parameters to the query.
        """
        self._limit = Limit(offset, num)
        return self

    def build_args(self) -> List[str]:
        args = []
        if self._combine:
            args.extend(("COMBINE", *self._combine))
        if self._load_fields:
            fields_str = " ".join(self._load_fields)
            fields = fields_str.split(" ")
            args.extend(("LOAD", len(fields), *fields))
        if self._groupby:
            args.extend(self._groupby)
        if self._apply:
            args.extend(self._apply)
        if self._sortby_fields:
            sortby_args = []
            for f in self._sortby_fields:
                sortby_args.extend(f.args)
            args.extend(("SORTBY", len(sortby_args), *sortby_args))
        if self._filter:
            args.extend(self._filter.args)
        if self._limit:
            args.extend(self._limit.build_args())

        return args


class HybridFilter(Filter):
    def __init__(
        self,
        conditions: str,
    ) -> None:
        args = [f"'{conditions}'"]  # need to wrap the conditions in quotes
        # args = [conditions]
        Filter.__init__(self, "FILTER", *args)


class HybridCursorQuery:
    def __init__(self, count: int = 0, max_idle: int = 0) -> None:
        self.count = count
        self.max_idle = max_idle

    def build_args(self):
        args = ["WITHCURSOR"]
        if self.count:
            args += ["COUNT", str(self.count)]
        if self.max_idle:
            args += ["MAXIDLE", str(self.max_idle)]
        return args
