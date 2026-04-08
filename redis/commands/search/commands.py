import itertools
import time
from typing import Any, Dict, List, Optional, Union

from redis._parsers.helpers import pairs_to_dict
from redis.client import NEVER_DECODE, Pipeline
from redis.commands.search.hybrid_query import (
    CombineResultsMethod,
    HybridCursorQuery,
    HybridPostProcessingConfig,
    HybridQuery,
)
from redis.commands.search.hybrid_result import HybridCursorResult, HybridResult
from redis.utils import (
    check_protocol_version,
    decode_field_value,
    deprecated_function,
    experimental_method,
    str_if_bytes,
)

from ..helpers import get_protocol_version
from .aggregation import (
    AggregateRequest,
    AggregateResult,
    Cursor,
)
from .document import Document
from .field import Field
from .index_definition import IndexDefinition
from .profile_information import ProfileInformation
from .query import Query
from .result import Result
from .suggestion import SuggestionParser

NUMERIC = "NUMERIC"

CREATE_CMD = "FT.CREATE"
ALTER_CMD = "FT.ALTER"
SEARCH_CMD = "FT.SEARCH"
ADD_CMD = "FT.ADD"
ADDHASH_CMD = "FT.ADDHASH"
DROPINDEX_CMD = "FT.DROPINDEX"
EXPLAIN_CMD = "FT.EXPLAIN"
EXPLAINCLI_CMD = "FT.EXPLAINCLI"
DEL_CMD = "FT.DEL"
AGGREGATE_CMD = "FT.AGGREGATE"
PROFILE_CMD = "FT.PROFILE"
CURSOR_CMD = "FT.CURSOR"
SPELLCHECK_CMD = "FT.SPELLCHECK"
DICT_ADD_CMD = "FT.DICTADD"
DICT_DEL_CMD = "FT.DICTDEL"
DICT_DUMP_CMD = "FT.DICTDUMP"
MGET_CMD = "FT.MGET"
CONFIG_CMD = "FT.CONFIG"
TAGVALS_CMD = "FT.TAGVALS"
ALIAS_ADD_CMD = "FT.ALIASADD"
ALIAS_UPDATE_CMD = "FT.ALIASUPDATE"
ALIAS_DEL_CMD = "FT.ALIASDEL"
INFO_CMD = "FT.INFO"
SUGADD_COMMAND = "FT.SUGADD"
SUGDEL_COMMAND = "FT.SUGDEL"
SUGLEN_COMMAND = "FT.SUGLEN"
SUGGET_COMMAND = "FT.SUGGET"
SYNUPDATE_CMD = "FT.SYNUPDATE"
SYNDUMP_CMD = "FT.SYNDUMP"
HYBRID_CMD = "FT.HYBRID"

NOOFFSETS = "NOOFFSETS"
NOFIELDS = "NOFIELDS"
NOHL = "NOHL"
NOFREQS = "NOFREQS"
MAXTEXTFIELDS = "MAXTEXTFIELDS"
TEMPORARY = "TEMPORARY"
STOPWORDS = "STOPWORDS"
SKIPINITIALSCAN = "SKIPINITIALSCAN"
WITHSCORES = "WITHSCORES"
FUZZY = "FUZZY"
WITHPAYLOADS = "WITHPAYLOADS"


class SearchCommands:
    """Search commands."""

    def _init_module_callbacks(self):
        """Build the RESP2/RESP3 module callback maps.

        Called from ``Search.__init__``, ``Pipeline.__init__`` and
        ``AsyncPipeline.__init__`` so that the mapping is defined in a
        single place rather than duplicated across all three classes.
        """
        self._RESP2_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info,
            SEARCH_CMD: self._parse_search,
            HYBRID_CMD: self._parse_hybrid_search,
            AGGREGATE_CMD: self._parse_aggregate,
            PROFILE_CMD: self._parse_profile,
            SPELLCHECK_CMD: self._parse_spellcheck,
            CONFIG_CMD: self._parse_config_get,
            SYNDUMP_CMD: self._parse_syndump,
        }
        self._RESP3_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info_resp3,
            SEARCH_CMD: self._parse_search_resp3,
            HYBRID_CMD: self._parse_hybrid_search_resp3,
            AGGREGATE_CMD: self._parse_aggregate_resp3,
            PROFILE_CMD: self._parse_profile_resp3,
            SPELLCHECK_CMD: self._parse_spellcheck_resp3,
            CONFIG_CMD: self._parse_config_get_resp3,
            SYNDUMP_CMD: self._parse_syndump_resp3,
        }

    # Commands whose parsers require a ``query`` kwarg.  When invoked as a
    # pipeline response-callback the kwarg is carried inside the options dict
    # that ``execute_command`` stored earlier.  If the key is absent (e.g. a
    # raw ``execute_command("FT.SEARCH", ...)`` call), return the response
    # unparsed so we don't crash.
    _QUERY_REQUIRED_CMDS = frozenset(
        {SEARCH_CMD, AGGREGATE_CMD, CURSOR_CMD, HYBRID_CMD, PROFILE_CMD}
    )

    def _parse_results(self, cmd, res, **kwargs):
        if cmd in self._QUERY_REQUIRED_CMDS and "query" not in kwargs:
            return res
        if check_protocol_version(get_protocol_version(self.client), 3):
            cb = self._RESP3_MODULE_CALLBACKS.get(cmd)
        else:
            cb = self._RESP2_MODULE_CALLBACKS.get(cmd)
        if cb:
            return cb(res, **kwargs)
        return res

    # ---- RESP2 parsers ----

    # Known FT.INFO attribute keys that are followed by a value (key-value pairs)
    _INFO_ATTR_PAIR_KEYS = frozenset(
        {"identifier", "attribute", "type", "WEIGHT", "SEPARATOR", "PHONETIC"}
    )

    def _parse_info(self, res, **kwargs):
        it = map(str_if_bytes, res)
        info = dict(zip(it, it))

        # Normalize RESP2 attributes from flat sublists to nested dicts
        # to match RESP3 format: [{"identifier": ..., "type": ..., "flags": [...]}]
        if "attributes" in info and isinstance(info["attributes"], list):
            info["attributes"] = [
                self._normalize_info_attribute(attr) if isinstance(attr, list) else attr
                for attr in info["attributes"]
            ]

        return info

    @staticmethod
    def _normalize_info_attribute(attr_list):
        """Convert a RESP2 flat attribute list to a RESP3-style dict.

        RESP2 format: [identifier, name, attribute, alias, type, TEXT, WEIGHT, 1,
                        SORTABLE, NOSTEM]
        RESP3 format: {"identifier": name, "attribute": alias, "type": "TEXT",
                        "WEIGHT": "1", "flags": ["SORTABLE", "NOSTEM"]}
        """
        result = {}
        flags = []
        pair_keys = SearchCommands._INFO_ATTR_PAIR_KEYS
        i = 0
        while i < len(attr_list):
            key = str_if_bytes(attr_list[i])
            if key in pair_keys and i + 1 < len(attr_list):
                result[key] = str_if_bytes(attr_list[i + 1])
                i += 2
            else:
                flags.append(key)
                i += 1
        result["flags"] = flags
        return result

    def _parse_search(self, res, **kwargs):
        return Result(
            res,
            not kwargs["query"]._no_content,
            duration=kwargs.get("duration", 0),
            has_payload=kwargs["query"]._with_payloads,
            with_scores=kwargs["query"]._with_scores,
            field_encodings=kwargs["query"]._return_fields_decode_as,
        )

    def _parse_hybrid_search(self, res, **kwargs):
        res_dict = pairs_to_dict(res, decode_keys=True)
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res_dict["SEARCH"]),
                vsim_cursor_id=int(res_dict["VSIM"]),
            )

        query = kwargs.get("query")
        field_encodings = (
            getattr(query, "_return_fields_decode_as", None) if query else None
        )

        results: List[Dict[str, Any]] = []
        # the original results are a list of lists
        # we convert them to a list of dicts
        for res_item in res_dict["results"]:
            item_dict = pairs_to_dict(res_item, decode_keys=True)
            results.append(
                {
                    k: decode_field_value(v, k, field_encodings)
                    for k, v in item_dict.items()
                }
            )

        return HybridResult(
            total_results=int(res_dict["total_results"]),
            results=results,
            warnings=[str_if_bytes(w) for w in res_dict["warnings"]],
            execution_time=float(res_dict["execution_time"]),
        )

    def _parse_aggregate(self, res, **kwargs):
        return self._get_aggregate_result(res, kwargs["query"], kwargs["has_cursor"])

    def _parse_profile(self, res, **kwargs):
        query = kwargs["query"]
        if isinstance(query, AggregateRequest):
            result = self._get_aggregate_result(res[0], query, query._cursor)
        else:
            result = Result(
                res[0],
                not query._no_content,
                duration=kwargs["duration"],
                has_payload=query._with_payloads,
                with_scores=query._with_scores,
            )

        profile_data = res[1]
        # >= 7.9.0 returns a flat key-value list ["Shards", [...], ...]
        # Convert to dict to match RESP3 format.
        # < 7.9.0 returns a list-of-pairs [[k, v], ...] where first element
        # is a list — leave as-is.
        if (
            isinstance(profile_data, list)
            and profile_data
            and isinstance(profile_data[0], (str, bytes))
        ):
            profile_data = pairs_to_dict(profile_data, decode_keys=True)

        return result, ProfileInformation(profile_data)

    def _parse_spellcheck(self, res, **kwargs):
        corrections = {}
        if res == 0:
            return corrections

        for _correction in res:
            if isinstance(_correction, int) and _correction == 0:
                continue

            if len(_correction) != 3:
                continue
            if not _correction[2]:
                continue
            if not _correction[2][0]:
                continue

            # For spellcheck output
            # 1)  1) "TERM"
            #     2) "{term1}"
            #     3)  1)  1)  "{score1}"
            #             2)  "{suggestion1}"
            #         2)  1)  "{score2}"
            #             2)  "{suggestion2}"
            #
            # Following dictionary will be made
            # corrections = {
            #     '{term1}': [
            #         {'score': '{score1}', 'suggestion': '{suggestion1}'},
            #         {'score': '{score2}', 'suggestion': '{suggestion2}'}
            #     ]
            # }
            corrections[_correction[1]] = [
                {"score": _item[0], "suggestion": _item[1]} for _item in _correction[2]
            ]

        return corrections

    def _parse_config_get(self, res, **kwargs):
        if not res:
            return {}
        return {str_if_bytes(kvs[0]): str_if_bytes(kvs[1]) for kvs in res}

    def _parse_syndump(self, res, **kwargs):
        if not res:
            return {}
        return {
            str_if_bytes(res[i]): [str_if_bytes(s) for s in res[i + 1]]
            if isinstance(res[i + 1], list)
            else str_if_bytes(res[i + 1])
            for i in range(0, len(res), 2)
        }

    # ---- RESP3 parsers ----

    def _parse_search_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SEARCH response into a Result object."""
        query = kwargs.get("query")
        return Result.from_resp3(
            res,
            duration=kwargs.get("duration", 0),
            with_scores=getattr(query, "_with_scores", False),
            field_encodings=getattr(query, "_return_fields_decode_as", None),
        )

    def _parse_aggregate_resp3(self, res, **kwargs):
        """Parse RESP3 FT.AGGREGATE response into an AggregateResult object."""
        query = kwargs.get("query")
        has_cursor = kwargs.get("has_cursor", False)

        # When has_cursor is True, RESP3 returns [data_dict, cursor_id]
        cursor_id = 0
        if has_cursor and isinstance(res, list):
            data = res[0]
            cursor_id = res[1] if len(res) > 1 else 0
        else:
            data = res

        warnings = [str_if_bytes(w) for w in data.get("warning", [])]
        total = data.get("total_results", 0)

        rows = []
        for result_item in data.get("results", []):
            extra_attrs = result_item.get("extra_attributes", {})
            # Convert dict to flat list [key, value, key, value, ...]
            # to match RESP2 row format
            flat = []
            for k, v in extra_attrs.items():
                flat.append(k)
                flat.append(v)
            rows.append(flat)

        cursor = None
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = cursor_id
                cursor = query
            else:
                cursor = Cursor(cursor_id)

        return AggregateResult(rows, cursor, None, total=total, warnings=warnings)

    def _parse_hybrid_search_resp3(self, res, **kwargs):
        """Parse RESP3 FT.HYBRID response into HybridResult/HybridCursorResult."""
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res["SEARCH"]),
                vsim_cursor_id=int(res["VSIM"]),
            )

        query = kwargs.get("query")
        field_encodings = (
            getattr(query, "_return_fields_decode_as", None) if query else None
        )

        results: List[Dict[str, Any]] = []
        for res_item in res.get("results", []):
            if isinstance(res_item, dict):
                results.append(
                    {
                        str_if_bytes(k): decode_field_value(
                            v, str_if_bytes(k), field_encodings
                        )
                        for k, v in res_item.items()
                    }
                )
            else:
                item_dict = pairs_to_dict(res_item, decode_keys=True)
                results.append(
                    {
                        k: decode_field_value(v, k, field_encodings)
                        for k, v in item_dict.items()
                    }
                )

        return HybridResult(
            total_results=int(res.get("total_results", 0)),
            results=results,
            warnings=[str_if_bytes(w) for w in res.get("warnings", [])],
            execution_time=float(res.get("execution_time", 0)),
        )

    def _parse_spellcheck_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SPELLCHECK response into unified format.

        RESP3 format:
            {"results": {"term": [{"suggestion": score}, ...], ...}}
        Unified format (matches RESP2 parsed output):
            {"term": [{"score": score_str, "suggestion": suggestion}, ...], ...}
        """
        corrections = {}
        if not isinstance(res, dict):
            return self._parse_spellcheck(res, **kwargs)
        results = res.get("results", {})
        for term, suggestions in results.items():
            if not suggestions:
                continue
            term_corrections = []
            for suggestion_dict in suggestions:
                for suggestion, score in suggestion_dict.items():
                    # Normalize score string to match RESP2 format:
                    # RESP3 returns float 0.0 → "0.0", but RESP2 returns "0"
                    score_str = str(score)
                    if score_str.endswith(".0"):
                        score_str = score_str[:-2]
                    term_corrections.append(
                        {"score": score_str, "suggestion": str(suggestion)}
                    )
            if term_corrections:
                corrections[term] = term_corrections
        return corrections

    def _parse_profile_resp3(self, res, **kwargs):
        """Parse RESP3 FT.PROFILE response into (result, ProfileInformation) tuple.

        RESP3 format (aligned, RediSearch >= MOD-6816):
            {"Results": {search/aggregate result dict},
             "Profile": {profile information dict}}

        Older RediSearch versions may return a list (same as RESP2) even
        when the connection uses RESP3.  In that case we delegate to the
        RESP2 ``_parse_profile`` parser.
        """
        # Older RediSearch returns a list even in RESP3 mode – fall back
        # to the RESP2 parser which already handles that format.
        if isinstance(res, list):
            return self._parse_profile(res, **kwargs)

        query = kwargs["query"]
        # RESP3 returns a dict with "Results" and "Profile" keys.
        # Handle both decoded (str) and raw (bytes) keys.
        # Use `is not None` instead of truthiness to avoid dropping falsy
        # values such as empty dicts/lists.
        results_data = res.get("Results")
        if results_data is None:
            results_data = res.get(b"Results")
        if results_data is None:
            results_data = res.get("results")
        if results_data is None:
            results_data = res.get(b"results")
        if results_data is None:
            results_data = res.get(0)
        profile_data = res.get("Profile")
        if profile_data is None:
            profile_data = res.get(b"Profile")
        if profile_data is None:
            profile_data = res.get("profile")
        if profile_data is None:
            profile_data = res.get(b"profile")
        if profile_data is None:
            profile_data = res.get(1)
        # On older servers (pre MOD-6816, e.g. Redis 7.2/7.4) the "Results"
        # value is a bare list of result-item dicts, not the wrapper dict
        # ``{"total_results": N, "results": [...], "warning": [...]}``.
        # Wrap the list so downstream parsers receive the expected format.
        if isinstance(results_data, list):
            results_data = {
                "total_results": len(results_data),
                "results": results_data,
            }
        if isinstance(query, AggregateRequest):
            result = self._parse_aggregate_resp3(
                results_data, query=query, has_cursor=bool(query._cursor)
            )
        else:
            result = Result.from_resp3(
                results_data,
                duration=kwargs.get("duration", 0),
                with_scores=getattr(query, "_with_scores", False),
            )
        # Normalize profile data keys/values to strings
        profile_data = self._to_string_recursive(profile_data)
        # On pre-7.9.0 servers, convert RESP3 profile dict to the RESP2
        # list-of-pairs format so consumers see a consistent structure
        # regardless of protocol.  Post-7.9.0 responses contain a "Shards"
        # key and are already handled uniformly by both parsers as dicts.
        if isinstance(profile_data, dict) and "Shards" not in profile_data:
            profile_data = self._resp3_profile_dict_to_list(profile_data)
        return result, ProfileInformation(profile_data)

    @staticmethod
    def _resp3_profile_dict_to_list(data):
        """Convert RESP3 profile dict into RESP2-style list-of-pairs.

        On pre-7.9.0 servers, RESP2 returns profile data as a list of
        ``[key, value]`` pairs where nested structures are flat alternating
        key-value lists.  RESP3 returns the same data as nested dicts.
        This converts the RESP3 dict back to the RESP2 list format so
        consumers see a consistent structure regardless of protocol.

        Key structural difference: when a dict value is a list of dicts
        (e.g. ``"Child iterators": [{...}, {...}]``), RESP2 expands each
        dict as a separate sibling element in the parent flat list rather
        than keeping them nested inside a single value.
        """

        def _is_list_of_dicts(obj):
            return isinstance(obj, list) and obj and isinstance(obj[0], dict)

        def _convert(obj, top_level=False):
            if isinstance(obj, dict):
                if top_level:
                    # Top-level: list of [key, ...values] entries
                    result = []
                    for k, v in obj.items():
                        entry = [k]
                        if _is_list_of_dicts(v):
                            for item in v:
                                entry.append(_convert(item))
                        else:
                            entry.append(_convert(v))
                        result.append(entry)
                    return result
                else:
                    # Nested: flat alternating key-value list
                    result = []
                    for k, v in obj.items():
                        result.append(k)
                        if _is_list_of_dicts(v):
                            for item in v:
                                result.append(_convert(item))
                        else:
                            result.append(_convert(v))
                    return result
            elif isinstance(obj, list):
                return [_convert(item) for item in obj]
            return obj

        return _convert(data, top_level=True)

    @staticmethod
    def _to_string_recursive(obj):
        """Recursively convert bytes keys/values to strings in nested structures.

        Handles dicts, lists, and scalar values. Non-bytes, non-container
        values (int, float, None, bool) are left unchanged.
        """
        if isinstance(obj, bytes):
            return str_if_bytes(obj)
        if isinstance(obj, dict):
            return {
                str_if_bytes(k) if isinstance(k, bytes) else k: (
                    SearchCommands._to_string_recursive(v)
                )
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [SearchCommands._to_string_recursive(item) for item in obj]
        return obj

    def _parse_info_resp3(self, res, **kwargs):
        """Parse RESP3 FT.INFO response, normalising bytes to strings.

        RESP3 returns a native dict with bytes keys/values. This converts
        all keys and string-like values to ``str`` so that the returned
        dict has the same shape as the RESP2 parser output.
        """
        return self._to_string_recursive(res)

    def _parse_config_get_resp3(self, res, **kwargs):
        """Parse RESP3 FT.CONFIG GET response, normalising bytes to strings.

        RESP3 already returns a dict, but keys and values are bytes.
        Convert them to match the RESP2 output format.
        """
        if not res:
            return {}
        return {str_if_bytes(k): str_if_bytes(v) for k, v in res.items()}

    def _parse_syndump_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SYNDUMP response, normalising bytes to strings.

        RESP3 already returns a dict, but keys and values are bytes.
        Convert them to match the RESP2 output format.
        """
        if not res:
            return {}
        return self._to_string_recursive(res)

    def batch_indexer(self, chunk_size=100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return self.BatchIndexer(self, chunk_size=chunk_size)

    def create_index(
        self,
        fields: List[Field],
        no_term_offsets: bool = False,
        no_field_flags: bool = False,
        stopwords: Optional[List[str]] = None,
        definition: Optional[IndexDefinition] = None,
        max_text_fields=False,
        temporary=None,
        no_highlight: bool = False,
        no_term_frequencies: bool = False,
        skip_initial_scan: bool = False,
    ):
        """
        Creates the search index. The index must not already exist.

        For more information, see https://redis.io/commands/ft.create/

        Args:
            fields: A list of Field objects.
            no_term_offsets: If `true`, term offsets will not be saved in the index.
            no_field_flags: If true, field flags that allow searching in specific fields
                            will not be saved.
            stopwords: If provided, the index will be created with this custom stopword
                       list. The list can be empty.
            definition: If provided, the index will be created with this custom index
                        definition.
            max_text_fields: If true, indexes will be encoded as if there were more than
                             32 text fields, allowing for additional fields beyond 32.
            temporary: Creates a lightweight temporary index which will expire after the
                       specified period of inactivity. The internal idle timer is reset
                       whenever the index is searched or added to.
            no_highlight: If true, disables highlighting support. Also implied by
                          `no_term_offsets`.
            no_term_frequencies: If true, term frequencies will not be saved in the
                                 index.
            skip_initial_scan: If true, the initial scan and indexing will be skipped.

        """
        args = [CREATE_CMD, self.index_name]
        if definition is not None:
            args += definition.args
        if max_text_fields:
            args.append(MAXTEXTFIELDS)
        if temporary is not None and isinstance(temporary, int):
            args.append(TEMPORARY)
            args.append(temporary)
        if no_term_offsets:
            args.append(NOOFFSETS)
        if no_highlight:
            args.append(NOHL)
        if no_field_flags:
            args.append(NOFIELDS)
        if no_term_frequencies:
            args.append(NOFREQS)
        if skip_initial_scan:
            args.append(SKIPINITIALSCAN)
        if stopwords is not None and isinstance(stopwords, (list, tuple, set)):
            args += [STOPWORDS, len(stopwords)]
            if len(stopwords) > 0:
                args += list(stopwords)

        args.append("SCHEMA")
        try:
            args += list(itertools.chain(*(f.redis_args() for f in fields)))
        except TypeError:
            args += fields.redis_args()

        return self.execute_command(*args)

    def alter_schema_add(self, fields: Union[Field, List[Field]]):
        """
        Alter the existing search index by adding new fields. The index
        must already exist.

        ### Parameters:

        - **fields**: a list of Field objects to add for the index

        For more information see `FT.ALTER <https://redis.io/commands/ft.alter>`_.
        """  # noqa

        args = [ALTER_CMD, self.index_name, "SCHEMA", "ADD"]
        try:
            args += list(itertools.chain(*(f.redis_args() for f in fields)))
        except TypeError:
            args += fields.redis_args()

        return self.execute_command(*args)

    def dropindex(self, delete_documents: bool = False):
        """
        Drop the index if it exists.
        Replaced `drop_index` in RediSearch 2.0.
        Default behavior was changed to not delete the indexed documents.

        ### Parameters:

        - **delete_documents**: If `True`, all documents will be deleted.

        For more information see `FT.DROPINDEX <https://redis.io/commands/ft.dropindex>`_.
        """  # noqa
        args = [DROPINDEX_CMD, self.index_name]

        delete_str = (
            "DD"
            if isinstance(delete_documents, bool) and delete_documents is True
            else ""
        )

        if delete_str:
            args.append(delete_str)

        return self.execute_command(*args)

    def _add_document(
        self,
        doc_id,
        conn=None,
        nosave=False,
        score=1.0,
        payload=None,
        replace=False,
        partial=False,
        language=None,
        no_create=False,
        **fields,
    ):
        """
        Internal add_document used for both batch and single doc indexing
        """

        if partial or no_create:
            replace = True

        args = [ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append("NOSAVE")
        if payload is not None:
            args.append("PAYLOAD")
            args.append(payload)
        if replace:
            args.append("REPLACE")
            if partial:
                args.append("PARTIAL")
            if no_create:
                args.append("NOCREATE")
        if language:
            args += ["LANGUAGE", language]
        args.append("FIELDS")
        args += list(itertools.chain(*fields.items()))

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    def _add_document_hash(
        self, doc_id, conn=None, score=1.0, language=None, replace=False
    ):
        """
        Internal add_document_hash used for both batch and single doc indexing
        """

        args = [ADDHASH_CMD, self.index_name, doc_id, score]

        if replace:
            args.append("REPLACE")

        if language:
            args += ["LANGUAGE", language]

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    @deprecated_function(
        version="2.0.0", reason="deprecated since redisearch 2.0, call hset instead"
    )
    def add_document(
        self,
        doc_id: str,
        nosave: bool = False,
        score: float = 1.0,
        payload: Optional[bool] = None,
        replace: bool = False,
        partial: bool = False,
        language: Optional[str] = None,
        no_create: bool = False,
        **fields: List[str],
    ):
        """
        Add a single document to the index.

        Args:

            doc_id: the id of the saved document.
            nosave: if set to true, we just index the document, and don't
                      save a copy of it. This means that searches will just
                      return ids.
            score: the document ranking, between 0.0 and 1.0
            payload: optional inner-index payload we can save for fast
                     access in scoring functions
            replace: if True, and the document already is in the index,
                     we perform an update and reindex the document
            partial: if True, the fields specified will be added to the
                       existing document.
                       This has the added benefit that any fields specified
                       with `no_index`
                       will not be reindexed again. Implies `replace`
            language: Specify the language used for document tokenization.
            no_create: if True, the document is only updated and reindexed
                         if it already exists.
                         If the document does not exist, an error will be
                         returned. Implies `replace`
            fields: kwargs dictionary of the document fields to be saved
                    and/or indexed.
                    NOTE: Geo points shoule be encoded as strings of "lon,lat"
        """  # noqa
        return self._add_document(
            doc_id,
            conn=None,
            nosave=nosave,
            score=score,
            payload=payload,
            replace=replace,
            partial=partial,
            language=language,
            no_create=no_create,
            **fields,
        )

    @deprecated_function(
        version="2.0.0", reason="deprecated since redisearch 2.0, call hset instead"
    )
    def add_document_hash(self, doc_id, score=1.0, language=None, replace=False):
        """
        Add a hash document to the index.

        ### Parameters

        - **doc_id**: the document's id. This has to be an existing HASH key
                      in Redis that will hold the fields the index needs.
        - **score**:  the document ranking, between 0.0 and 1.0
        - **replace**: if True, and the document already is in the index, we
                      perform an update and reindex the document
        - **language**: Specify the language used for document tokenization.
        """  # noqa
        return self._add_document_hash(
            doc_id, conn=None, score=score, language=language, replace=replace
        )

    @deprecated_function(version="2.0.0", reason="deprecated since redisearch 2.0")
    def delete_document(self, doc_id, conn=None, delete_actual_document=False):
        """
        Delete a document from index
        Returns 1 if the document was deleted, 0 if not

        ### Parameters

        - **delete_actual_document**: if set to True, RediSearch also delete
                                      the actual document if it is in the index
        """  # noqa
        args = [DEL_CMD, self.index_name, doc_id]
        if delete_actual_document:
            args.append("DD")

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    def load_document(self, id, field_encodings=None):
        """
        Load a single document by id

        - **field_encodings**: optional dict mapping field names to encodings.
          If a field's encoding is ``None`` the raw bytes value is preserved
          (useful for binary data such as vectors).
        """
        fields = self.client.hgetall(id)
        fields = {
            str_if_bytes(k): decode_field_value(v, str_if_bytes(k), field_encodings)
            for k, v in fields.items()
        }

        try:
            del fields["id"]
        except KeyError:
            pass

        return Document(id=id, **fields)

    @deprecated_function(version="2.0.0", reason="deprecated since redisearch 2.0")
    def get(self, *ids):
        """
        Returns the full contents of multiple documents.

        ### Parameters

        - **ids**: the ids of the saved documents.

        """

        return self.execute_command(MGET_CMD, self.index_name, *ids)

    def info(self):
        """
        Get info an stats about the the current index, including the number of
        documents, memory consumption, etc

        For more information see `FT.INFO <https://redis.io/commands/ft.info>`_.
        """

        res = self.execute_command(INFO_CMD, self.index_name)
        return self._parse_results(INFO_CMD, res)

    def get_params_args(
        self, query_params: Optional[Dict[str, Union[str, int, float, bytes]]]
    ):
        if query_params is None:
            return []
        args = []
        if len(query_params) > 0:
            args.append("PARAMS")
            args.append(len(query_params) * 2)
            for key, value in query_params.items():
                args.append(key)
                args.append(value)
        return args

    def _mk_query_args(
        self, query, query_params: Optional[Dict[str, Union[str, int, float, bytes]]]
    ):
        args = [self.index_name]

        if isinstance(query, str):
            # convert the query from a text to a query object
            query = Query(query)
        if not isinstance(query, Query):
            raise ValueError(f"Bad query type {type(query)}")

        args += query.get_args()
        args += self.get_params_args(query_params)

        return args, query

    def search(
        self,
        query: Union[str, Query],
        query_params: Union[Dict[str, Union[str, int, float, bytes]], None] = None,
    ):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with
                     default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format

        For more information see `FT.SEARCH <https://redis.io/commands/ft.search>`_.
        """  # noqa
        args, query = self._mk_query_args(query, query_params=query_params)
        st = time.monotonic()

        options = {}
        if get_protocol_version(self.client) not in ["3", 3]:
            options[NEVER_DECODE] = True
        options["query"] = query

        res = self.execute_command(SEARCH_CMD, *args, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(
            SEARCH_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    @experimental_method()
    def hybrid_search(
        self,
        query: HybridQuery,
        combine_method: Optional[CombineResultsMethod] = None,
        post_processing: Optional[HybridPostProcessingConfig] = None,
        params_substitution: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
        timeout: Optional[int] = None,
        cursor: Optional[HybridCursorQuery] = None,
    ) -> Union[HybridResult, HybridCursorResult, Pipeline]:
        """
        Execute a hybrid search using both text and vector queries

        Args:
            - **query**: HybridQuery object
                        Contains the text and vector queries
            - **combine_method**: CombineResultsMethod object
                        Contains the combine method and parameters
            - **post_processing**: HybridPostProcessingConfig object
                        Contains the post processing configuration
            - **params_substitution**: Dict[str, Union[str, int, float, bytes]]
                        Contains the parameters substitution
            - **timeout**: int - contains the timeout in milliseconds
            - **cursor**: HybridCursorQuery object - contains the cursor configuration


        For more information see `FT.SEARCH <https://redis.io/commands/ft.hybrid>`.
        """
        index = self.index_name
        options = {}
        pieces = [HYBRID_CMD, index]
        pieces.extend(query.get_args())
        if combine_method:
            pieces.extend(combine_method.get_args())
        if post_processing:
            pieces.extend(post_processing.build_args())
        if params_substitution:
            pieces.extend(self.get_params_args(params_substitution))
        if timeout:
            pieces.extend(("TIMEOUT", timeout))
        if cursor:
            options["cursor"] = True
            pieces.extend(cursor.build_args())

        if get_protocol_version(self.client) not in ["3", 3]:
            options[NEVER_DECODE] = True
        options["query"] = query

        res = self.execute_command(*pieces, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(HYBRID_CMD, res, **options)

    def explain(
        self,
        query: Union[str, Query],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """Returns the execution plan for a complex query.

        For more information see `FT.EXPLAIN <https://redis.io/commands/ft.explain>`_.
        """  # noqa
        args, query_text = self._mk_query_args(query, query_params=query_params)
        return self.execute_command(EXPLAIN_CMD, *args)

    def explain_cli(self, query: Union[str, Query]):  # noqa
        raise NotImplementedError("EXPLAINCLI will not be implemented.")

    def aggregate(
        self,
        query: Union[AggregateRequest, Cursor],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Issue an aggregation query.

        ### Parameters

        **query**: This can be either an `AggregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from
        its `rows` property, which will always yield the rows of the result.

        For more information see `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`_.
        """  # noqa
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)
        cmd += self.get_params_args(query_params)

        raw = self.execute_command(*cmd, query=query, has_cursor=has_cursor)

        if isinstance(raw, Pipeline):
            return raw

        return self._parse_results(
            AGGREGATE_CMD, raw, query=query, has_cursor=has_cursor
        )

    def _get_aggregate_result(
        self, raw: List, query: Union[AggregateRequest, Cursor], has_cursor: bool
    ):
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = raw[1]
                cursor = query
            else:
                cursor = Cursor(raw[1])
            raw = raw[0]
        else:
            cursor = None

        if isinstance(query, AggregateRequest) and query._with_schema:
            schema = raw[0]
            rows = raw[2:]
        else:
            schema = None
            rows = raw[1:]

        return AggregateResult(rows, cursor, schema)

    def profile(
        self,
        query: Union[Query, AggregateRequest],
        limited: bool = False,
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Performs a search or aggregate command and collects performance
        information.

        ### Parameters

        **query**: This can be either an `AggregateRequest` or `Query`.
        **limited**: If set to True, removes details of reader iterator.
        **query_params**: Define one or more value parameters.
        Each parameter has a name and a value.

        """
        st = time.monotonic()
        cmd = [PROFILE_CMD, self.index_name, ""]
        if limited:
            cmd.append("LIMITED")
        cmd.append("QUERY")

        if isinstance(query, AggregateRequest):
            cmd[2] = "AGGREGATE"
            cmd += query.build_args()
        elif isinstance(query, Query):
            cmd[2] = "SEARCH"
            cmd += query.get_args()
            cmd += self.get_params_args(query_params)
        else:
            raise ValueError("Must provide AggregateRequest object or Query object.")

        res = self.execute_command(*cmd)

        return self._parse_results(
            PROFILE_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        Args:

            query: search query.
            distance: the maximal Levenshtein distance for spelling
                       suggestions (default: 1, max: 4).
            include: specifies an inclusion custom dictionary.
            exclude: specifies an exclusion custom dictionary.

        For more information see `FT.SPELLCHECK <https://redis.io/commands/ft.spellcheck>`_.
        """  # noqa
        cmd = [SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(["DISTANCE", distance])

        if include:
            cmd.extend(["TERMS", "INCLUDE", include])

        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        res = self.execute_command(*cmd)

        return self._parse_results(SPELLCHECK_CMD, res)

    def dict_add(self, name: str, *terms: List[str]):
        """Adds terms to a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for adding to the dictionary.

        For more information see `FT.DICTADD <https://redis.io/commands/ft.dictadd>`_.
        """  # noqa
        cmd = [DICT_ADD_CMD, name]
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def dict_del(self, name: str, *terms: List[str]):
        """Deletes terms from a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for removing from the dictionary.

        For more information see `FT.DICTDEL <https://redis.io/commands/ft.dictdel>`_.
        """  # noqa
        cmd = [DICT_DEL_CMD, name]
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def dict_dump(self, name: str):
        """Dumps all terms in the given dictionary.

        ### Parameters

        - **name**: Dictionary name.

        For more information see `FT.DICTDUMP <https://redis.io/commands/ft.dictdump>`_.
        """  # noqa
        cmd = [DICT_DUMP_CMD, name]
        return self.execute_command(*cmd)

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_set from core module instead",
    )
    def config_set(self, option: str, value: str) -> bool:
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.

        For more information see `FT.CONFIG SET <https://redis.io/commands/ft.config-set>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = self.execute_command(*cmd)
        return raw == "OK"

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_get from core module instead",
    )
    def config_get(self, option: str) -> str:
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.

        For more information see `FT.CONFIG GET <https://redis.io/commands/ft.config-get>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "GET", option]
        res = self.execute_command(*cmd)
        return self._parse_results(CONFIG_CMD, res)

    def tagvals(self, tagfield: str):
        """
        Return a list of all possible tag values

        ### Parameters

        - **tagfield**: Tag field name

        For more information see `FT.TAGVALS <https://redis.io/commands/ft.tagvals>`_.
        """  # noqa

        return self.execute_command(TAGVALS_CMD, self.index_name, tagfield)

    def aliasadd(self, alias: str):
        """
        Alias a search index - will fail if alias already exists

        ### Parameters

        - **alias**: Name of the alias to create

        For more information see `FT.ALIASADD <https://redis.io/commands/ft.aliasadd>`_.
        """  # noqa

        return self.execute_command(ALIAS_ADD_CMD, alias, self.index_name)

    def aliasupdate(self, alias: str):
        """
        Updates an alias - will fail if alias does not already exist

        ### Parameters

        - **alias**: Name of the alias to create

        For more information see `FT.ALIASUPDATE <https://redis.io/commands/ft.aliasupdate>`_.
        """  # noqa

        return self.execute_command(ALIAS_UPDATE_CMD, alias, self.index_name)

    def aliasdel(self, alias: str):
        """
        Removes an alias to a search index

        ### Parameters

        - **alias**: Name of the alias to delete

        For more information see `FT.ALIASDEL <https://redis.io/commands/ft.aliasdel>`_.
        """  # noqa
        return self.execute_command(ALIAS_DEL_CMD, alias)

    def sugadd(self, key, *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has
        a score and string.
        If kwargs["increment"] is true and the terms are already in the
        server's dictionary, we increment their scores.

        For more information see `FT.SUGADD <https://redis.io/commands/ft.sugadd/>`_.
        """  # noqa
        # If Transaction is not False it will MULTI/EXEC which will error
        pipe = self.pipeline(transaction=False)
        for sug in suggestions:
            args = [SUGADD_COMMAND, key, sug.string, sug.score]
            if kwargs.get("increment"):
                args.append("INCR")
            if sug.payload:
                args.append("PAYLOAD")
                args.append(sug.payload)

            pipe.execute_command(*args)

        return pipe.execute()[-1]

    def suglen(self, key: str) -> int:
        """
        Return the number of entries in the AutoCompleter index.

        For more information see `FT.SUGLEN <https://redis.io/commands/ft.suglen>`_.
        """  # noqa
        return self.execute_command(SUGLEN_COMMAND, key)

    def sugdel(self, key: str, string: str) -> int:
        """
        Delete a string from the AutoCompleter index.
        Returns 1 if the string was found and deleted, 0 otherwise.

        For more information see `FT.SUGDEL <https://redis.io/commands/ft.sugdel>`_.
        """  # noqa
        return self.execute_command(SUGDEL_COMMAND, key, string)

    def sugget(
        self,
        key: str,
        prefix: str,
        fuzzy: bool = False,
        num: int = 10,
        with_scores: bool = False,
        with_payloads: bool = False,
    ) -> List[SuggestionParser]:
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix.

        Parameters:

        prefix : str
            The prefix we are searching. **Must be valid ascii or utf-8**
        fuzzy : bool
            If set to true, the prefix search is done in fuzzy mode.
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes
            can be very
            slow, and even scan the entire index.
        with_scores : bool
            If set to true, we also return the (refactored) score of
            each suggestion.
            This is normally not needed, and is NOT the original score
            inserted into the index.
        with_payloads : bool
            Return suggestion payloads
        num : int
            The maximum number of results we return. Note that we might
            return less. The algorithm trims irrelevant suggestions.

        Returns:

        list:
             A list of Suggestion objects. If with_scores was False, the
             score of all suggestions is 1.

        For more information see `FT.SUGGET <https://redis.io/commands/ft.sugget>`_.
        """  # noqa
        args = [SUGGET_COMMAND, key, prefix, "MAX", num]
        if fuzzy:
            args.append(FUZZY)
        if with_scores:
            args.append(WITHSCORES)
        if with_payloads:
            args.append(WITHPAYLOADS)

        res = self.execute_command(*args)
        results = []
        if not res:
            return results

        parser = SuggestionParser(with_scores, with_payloads, res)
        return [s for s in parser]

    def synupdate(self, groupid: str, skipinitial: bool = False, *terms: List[str]):
        """
        Updates a synonym group.
        The command is used to create or update a synonym group with
        additional terms.
        Only documents which were indexed after the update will be affected.

        Parameters:

        groupid :
            Synonym group id.
        skipinitial : bool
            If set to true, we do not scan and index.
        terms :
            The terms.

        For more information see `FT.SYNUPDATE <https://redis.io/commands/ft.synupdate>`_.
        """  # noqa
        cmd = [SYNUPDATE_CMD, self.index_name, groupid]
        if skipinitial:
            cmd.extend(["SKIPINITIALSCAN"])
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def syndump(self):
        """
        Dumps the contents of a synonym group.

        The command is used to dump the synonyms data structure.
        Returns a list of synonym terms and their synonym group ids.

        For more information see `FT.SYNDUMP <https://redis.io/commands/ft.syndump>`_.
        """  # noqa
        res = self.execute_command(SYNDUMP_CMD, self.index_name)
        return self._parse_results(SYNDUMP_CMD, res)


class AsyncSearchCommands(SearchCommands):
    async def info(self):
        """
        Get info an stats about the the current index, including the number of
        documents, memory consumption, etc

        For more information see `FT.INFO <https://redis.io/commands/ft.info>`_.
        """

        res = await self.execute_command(INFO_CMD, self.index_name)
        return self._parse_results(INFO_CMD, res)

    async def search(
        self,
        query: Union[str, Query],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with
                     default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format

        For more information see `FT.SEARCH <https://redis.io/commands/ft.search>`_.
        """  # noqa
        args, query = self._mk_query_args(query, query_params=query_params)
        st = time.monotonic()

        options = {}
        if get_protocol_version(self.client) not in ["3", 3]:
            options[NEVER_DECODE] = True
        options["query"] = query

        res = await self.execute_command(SEARCH_CMD, *args, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(
            SEARCH_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    @experimental_method()
    async def hybrid_search(
        self,
        query: HybridQuery,
        combine_method: Optional[CombineResultsMethod] = None,
        post_processing: Optional[HybridPostProcessingConfig] = None,
        params_substitution: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
        timeout: Optional[int] = None,
        cursor: Optional[HybridCursorQuery] = None,
    ) -> Union[HybridResult, HybridCursorResult, Pipeline]:
        """
        Execute a hybrid search using both text and vector queries

        Args:
            - **query**: HybridQuery object
                        Contains the text and vector queries
            - **combine_method**: CombineResultsMethod object
                        Contains the combine method and parameters
            - **post_processing**: HybridPostProcessingConfig object
                        Contains the post processing configuration
            - **params_substitution**: Dict[str, Union[str, int, float, bytes]]
                        Contains the parameters substitution
            - **timeout**: int - contains the timeout in milliseconds
            - **cursor**: HybridCursorQuery object - contains the cursor configuration


        For more information see `FT.SEARCH <https://redis.io/commands/ft.hybrid>`.
        """
        index = self.index_name
        options = {}
        pieces = [HYBRID_CMD, index]
        pieces.extend(query.get_args())
        if combine_method:
            pieces.extend(combine_method.get_args())
        if post_processing:
            pieces.extend(post_processing.build_args())
        if params_substitution:
            pieces.extend(self.get_params_args(params_substitution))
        if timeout:
            pieces.extend(("TIMEOUT", timeout))
        if cursor:
            options["cursor"] = True
            pieces.extend(cursor.build_args())

        if get_protocol_version(self.client) not in ["3", 3]:
            options[NEVER_DECODE] = True
        options["query"] = query

        res = await self.execute_command(*pieces, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(HYBRID_CMD, res, **options)

    async def aggregate(
        self,
        query: Union[AggregateResult, Cursor],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Issue an aggregation query.

        ### Parameters

        **query**: This can be either an `AggregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from
        its `rows` property, which will always yield the rows of the result.

        For more information see `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`_.
        """  # noqa
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)
        cmd += self.get_params_args(query_params)

        raw = await self.execute_command(*cmd, query=query, has_cursor=has_cursor)

        if isinstance(raw, Pipeline):
            return raw

        return self._parse_results(
            AGGREGATE_CMD, raw, query=query, has_cursor=has_cursor
        )

    async def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        ### Parameters

        **query**: search query.
        **distance***: the maximal Levenshtein distance for spelling
                       suggestions (default: 1, max: 4).
        **include**: specifies an inclusion custom dictionary.
        **exclude**: specifies an exclusion custom dictionary.

        For more information see `FT.SPELLCHECK <https://redis.io/commands/ft.spellcheck>`_.
        """  # noqa
        cmd = [SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(["DISTANCE", distance])

        if include:
            cmd.extend(["TERMS", "INCLUDE", include])

        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        res = await self.execute_command(*cmd)

        return self._parse_results(SPELLCHECK_CMD, res)

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_set from core module instead",
    )
    async def config_set(self, option: str, value: str) -> bool:
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.

        For more information see `FT.CONFIG SET <https://redis.io/commands/ft.config-set>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = await self.execute_command(*cmd)
        return raw == "OK"

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_get from core module instead",
    )
    async def config_get(self, option: str) -> str:
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.

        For more information see `FT.CONFIG GET <https://redis.io/commands/ft.config-get>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "GET", option]
        res = {}
        res = await self.execute_command(*cmd)
        return self._parse_results(CONFIG_CMD, res)

    async def load_document(self, id, field_encodings=None):
        """
        Load a single document by id

        - **field_encodings**: optional dict mapping field names to encodings.
          If a field's encoding is ``None`` the raw bytes value is preserved
          (useful for binary data such as vectors).
        """
        fields = await self.client.hgetall(id)
        fields = {
            str_if_bytes(k): decode_field_value(v, str_if_bytes(k), field_encodings)
            for k, v in fields.items()
        }

        try:
            del fields["id"]
        except KeyError:
            pass

        return Document(id=id, **fields)

    async def sugadd(self, key, *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has
        a score and string.
        If kwargs["increment"] is true and the terms are already in the
        server's dictionary, we increment their scores.

        For more information see `FT.SUGADD <https://redis.io/commands/ft.sugadd>`_.
        """  # noqa
        # If Transaction is not False it will MULTI/EXEC which will error
        pipe = self.pipeline(transaction=False)
        for sug in suggestions:
            args = [SUGADD_COMMAND, key, sug.string, sug.score]
            if kwargs.get("increment"):
                args.append("INCR")
            if sug.payload:
                args.append("PAYLOAD")
                args.append(sug.payload)

            pipe.execute_command(*args)

        return (await pipe.execute())[-1]

    async def sugget(
        self,
        key: str,
        prefix: str,
        fuzzy: bool = False,
        num: int = 10,
        with_scores: bool = False,
        with_payloads: bool = False,
    ) -> List[SuggestionParser]:
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix.

        Parameters:

        prefix : str
            The prefix we are searching. **Must be valid ascii or utf-8**
        fuzzy : bool
            If set to true, the prefix search is done in fuzzy mode.
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes
            can be very
            slow, and even scan the entire index.
        with_scores : bool
            If set to true, we also return the (refactored) score of
            each suggestion.
            This is normally not needed, and is NOT the original score
            inserted into the index.
        with_payloads : bool
            Return suggestion payloads
        num : int
            The maximum number of results we return. Note that we might
            return less. The algorithm trims irrelevant suggestions.

        Returns:

        list:
             A list of Suggestion objects. If with_scores was False, the
             score of all suggestions is 1.

        For more information see `FT.SUGGET <https://redis.io/commands/ft.sugget>`_.
        """  # noqa
        args = [SUGGET_COMMAND, key, prefix, "MAX", num]
        if fuzzy:
            args.append(FUZZY)
        if with_scores:
            args.append(WITHSCORES)
        if with_payloads:
            args.append(WITHPAYLOADS)

        ret = await self.execute_command(*args)
        results = []
        if not ret:
            return results

        parser = SuggestionParser(with_scores, with_payloads, ret)
        return [s for s in parser]
