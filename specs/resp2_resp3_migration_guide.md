# Migration Guide: redis-py 8.0+ Response Unification

Starting with redis-py 8.0, responses are **unified across RESP2 and RESP3 protocols**. The same command now returns the same Python type and structure regardless of which protocol version you use. This eliminates the need for protocol-specific branching in application code.

> **~84 commands** are affected across core Redis, Search, JSON, TimeSeries, and Probabilistic modules.
>
> ⚠️ **All changes listed below are breaking changes.**

---

## Section 1: Users on RESP2 (protocol=2, the default)

If you are using RESP2 (the default protocol), the following response formats have changed.

### Core Commands

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **ZDIFF** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"a", b"1")]` | `list[list[val, score]]` — scores as float | `[[b"a", 1.0]]` |
| 2 | **ZINTER** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"a", b"1")]` | `list[list[val, score]]` — scores as float | `[[b"a", 1.0]]` |
| 3 | **ZPOPMAX** | Tuples → lists; scores → float | `list[tuple(val, score)]` | `[(b"a", 3)]` | `list[list[val, score]]` — scores as float | `[[b"a", 3.0]]` |
| 4 | **ZPOPMIN** | Tuples → lists; scores → float | `list[tuple(val, score)]` | `[(b"a", 3)]` | `list[list[val, score]]` — scores as float | `[[b"a", 3.0]]` |
| 5 | **ZRANGE** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"a", b"1"), (b"b", b"2")]` | `list[list[val, score]]` — scores as float | `[[b"a", 1.0], [b"b", 2.0]]` |
| 6 | **ZRANGEBYSCORE** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"a", b"1")]` | `list[list[val, score]]` — scores as float | `[[b"a", 1.0]]` |
| 7 | **ZREVRANGE** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"b", b"2")]` | `list[list[val, score]]` — scores as float | `[[b"b", 2.0]]` |
| 8 | **ZREVRANGEBYSCORE** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"b", b"2")]` | `list[list[val, score]]` — scores as float | `[[b"b", 2.0]]` |
| 9 | **ZUNION** (withscores) | Tuples → lists; scores bytes → float | `list[tuple(val, score)]` — scores as bytes | `[(b"a", b"1")]` | `list[list[val, score]]` — scores as float | `[[b"a", 1.0]]` |
| 10 | **ZRANDMEMBER** (withscores) | Flat interleaved list → nested pairs; scores → float | Flat interleaved `list` (len=2N) | `[b"a", b"1", b"b", b"2"]` | `list[list[val, score]]` (len=N) | `[[b"a", 1.0], [b"b", 2.0]]` |
| 11 | **HRANDFIELD** (withvalues) | Flat interleaved list → nested pairs | Flat interleaved `list` (len=2N) | `[b"f1", b"v1", b"f2", b"v2"]` | `list[list[field, value]]` (len=N) | `[[b"f1", b"v1"], [b"f2", b"v2"]]` |
| 12 | **BLPOP** | Tuple → list | `tuple(key, val)` | `(b"key", b"val")` | `list[key, val]` | `[b"key", b"val"]` |
| 13 | **BRPOP** | Tuple → list | `tuple(key, val)` | `(b"key", b"val")` | `list[key, val]` | `[b"key", b"val"]` |
| 14 | **BZPOPMAX** | Tuple → list; score as float | `tuple(key, member, score)` | `(b"key", b"member", 3)` | `list[key, member, float]` | `[b"key", b"member", 3.0]` |
| 15 | **BZPOPMIN** | Tuple → list; score as float | `tuple(key, member, score)` | `(b"key", b"member", 1)` | `list[key, member, float]` | `[b"key", b"member", 1.0]` |
| 16 | **ZMPOP** | Scores bytes → float; structure normalized | Scores as bytes strings | `[b"key", [(b"a", b"1")]]` | Scores as float, nested lists | `[b"key", [[b"a", 1.0]]]` |
| 17 | **BZMPOP** | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP |
| 18 | **ZRANK/ZREVRANK** (withscore) | Score int → float | `[int, int]` | `[2, 3]` | `[int, float]` | `[2, 3.0]` |
| 19 | **ZSCAN** | Score type passed to cast func changed | Scores passed as bytes | `score_cast_func(b"1")` | Scores passed as float | `score_cast_func(1.0)` |
| 20 | **LCS** (with IDX) | Flat list → dict | Flat key-value list | `[b"matches", [...], b"len", 6]` | `dict` with string keys | `{"matches": [...], "len": 6}` |
| 21 | **CLUSTER GETKEYSINSLOT** | `str` → `bytes` (when `decode_responses=False`) | `list[str]` | `["key1", "key2"]` | `list[bytes]` | `[b"key1", b"key2"]` |
| 22 | **COMMAND GETKEYS** | `str` → `bytes` (when `decode_responses=False`) | `list[str]` | `["a", "c", "e"]` | `list[bytes]` | `[b"a", b"c", b"e"]` |
| 23 | **XREAD** | List of lists → dict | `list[list[stream, entries]]` (empty: `[]`) | `[[b"stream", [(id, fields)]]]` | `dict[stream, entries]` (empty: `{}`) | `{b"stream": [(id, fields)]}` |
| 24 | **XREADGROUP** | List of lists → dict | `list[list[stream, entries]]` | `[[b"stream", [(id, fields)]]]` | `dict[stream, entries]` | `{b"stream": [(id, fields)]}` |
| 25 | **CLIENT TRACKINGINFO** | Flat list → dict | Flat key-value list | `["flags", [...], "redirect", -1, ...]` | `dict` with string keys | `{"flags": [...], "redirect": -1, ...}` |
| 26 | **GEOPOS** | Tuples → lists for coordinates | `list[tuple \| None]` | `[(1.0, 2.0), None]` | `list[list \| None]` | `[[1.0, 2.0], None]` |
| 27 | **STRALGO** (IDX) | Tuples → lists for positions | `dict` with tuple positions | `{"matches": [((0,2),(0,2))], "len": 3}` | `dict` with list positions | `{"matches": [[[0,2],[0,2]]], "len": 3}` |
| 28 | **SENTINEL MASTER** | Flags comma-string → set | `dict` with `flags` as comma-string | `{"flags": "master,odown"}` | `dict` with `flags` as `set` | `{"flags": {"master", "odown"}}` |
| 29 | **SENTINEL MASTERS** | Flags comma-string → set | `dict[name, dict]` with comma-string flags | `{"mymaster": {"flags": "master"}}` | `dict[name, dict]` with `set` flags | `{"mymaster": {"flags": {"master"}}}` |
| 30 | **SENTINEL SENTINELS** | Flags comma-string → set | `list[dict]` with comma-string flags | `[{"flags": "sentinel"}]` | `list[dict]` with `set` flags | `[{"flags": {"sentinel"}}]` |
| 31 | **SENTINEL SLAVES** | Flags comma-string → set | `list[dict]` with comma-string flags | `[{"flags": "slave"}]` | `list[dict]` with `set` flags | `[{"flags": {"slave"}}]` |
| 32 | **COMMAND** | Flags list → set; add `acl_categories` | `dict` with `flags` as `list` | `{"get": {"flags": ["readonly", ...]}}` | `dict` with `flags` as `set`; new `acl_categories` | `{"get": {"flags": {"readonly", ...}, "acl_categories": {...}}}` |
| 33 | **ACL GETUSER** | Selectors flat lists → dicts | Selectors as flat lists | `{"selectors": [["~key", "+get"]]}` | Selectors as dicts | `{"selectors": [{"keys": "~key", "commands": "+get"}]}` |
| 34 | **XINFO CONSUMERS** | All values as bytes → native types | `dict` with bytes keys/values | `{b"idle": b"1234"}` | `dict` with string keys, native types | `{"idle": 1234}` |
| 35 | **XINFO GROUPS** | All values as bytes → native types | `dict` with bytes keys/values | `{b"consumers": b"2"}` | `dict` with string keys, native types | `{"consumers": 2}` |
| 36 | **MEMORY STATS** | String values → native int types | `dict` with string values | `{"peak.allocated": "1024"}` | `dict` with int values | `{"peak.allocated": 1024}` |
| 37 | **FUNCTION LIST** | Flat key-value lists → nested dicts | Flat key-value sublists | `[[b"library_name", b"mylib", ...]]` | `list[dict]` with nested dicts | `[{b"library_name": b"mylib", b"functions": [{b"name": b"myfunc", ...}]}]` |
| 38 | **CLUSTER LINKS** | Flat lists → dicts; bytes keys → string keys | Flat lists per link | `[[b"direction", b"to", ...]]` | `list[dict]` with string keys | `[{"direction": b"to", ...}]` |
| 39 | **CLUSTER SHARDS** | Bytes keys → string keys at node level | Node attrs with bytes keys | `{"nodes": [{b"id": b"abc"}]}` | Node attrs with string keys | `{"nodes": [{"id": b"abc"}]}` |

### Probabilistic Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **TOPK.ADD** | Remove incorrect int/float coercion | Numeric strings coerced to int/float | `"42"` → `42` | Numeric strings preserved as-is | `"42"` stays `"42"` |
| 2 | **TOPK.INCRBY** | Remove incorrect int/float coercion | Numeric strings coerced to int/float | `"42"` → `42` | Numeric strings preserved as-is | `"42"` stays `"42"` |
| 3 | **TOPK.LIST** | Remove incorrect int/float coercion | Numeric strings coerced to int/float | `"42"` → `42` | Numeric strings preserved as-is | `"42"` stays `"42"` |

### TimeSeries Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **TS.GET** | Tuple → list | `tuple(timestamp, value)` | `(1234567890, 1.5)` | `list[timestamp, value]` | `[1234567890, 1.5]` |
| 2 | **TS.RANGE** | List of tuples → list of lists | `list[tuple(ts, val)]` | `[(ts, val), ...]` | `list[list[ts, val]]` | `[[ts, val], ...]` |
| 3 | **TS.REVRANGE** | List of tuples → list of lists | `list[tuple(ts, val)]` | `[(ts, val), ...]` | `list[list[ts, val]]` | `[[ts, val], ...]` |
| 4 | **TS.QUERYINDEX** | Remove incorrect int coercion | Numeric key strings coerced to int | `"2"` → `2` | Key strings preserved as-is | `"2"` stays `"2"` |
| 5 | **TS.MGET** | Sorted list of dicts → dict | Sorted `list[dict]` | `[{"1": [{}, None, None]}]` | `dict[key, [labels, sample]]` | `{"1": [{}, [ts, val]]}` |
| 6 | **TS.MRANGE** | Sorted list of dicts → dict; add metadata slot | Sorted `list[dict]` with 2-element value | `[{"1": [labels, samples]}]` | `dict` with 3-element value (labels, metadata, samples) | `{"1": [labels, metadata, samples]}` |
| 7 | **TS.MREVRANGE** | Same as TS.MRANGE | Same as TS.MRANGE | Same as TS.MRANGE | Same as TS.MRANGE | Same as TS.MRANGE |

### JSON Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **JSON.NUMINCRBY** | Scalar → array for legacy paths | Scalar value | `5` | Array-wrapped value | `[5]` |
| 2 | **JSON.NUMMULTBY** | Scalar → array for legacy paths | Scalar value | `10` | Array-wrapped value | `[10]` |
| 3 | **JSON.RESP** | String-encoded floats → native floats | Floats as strings in nested lists | `[b"{", b"price", b"-19.5"]` | Floats as native Python floats | `[b"{", b"price", -19.5]` |
| 4 | **JSON.OBJKEYS** | Remove `nativestr` — respect `decode_responses` | Keys forced to `str` | `["key1", "key2"]` | Keys as `bytes` when `decode_responses=False` | `[b"key1", b"key2"]` |

### Search (FT) Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **FT.INFO** | Flat attribute sublists → structured dicts | Attributes as flat sublists | `{"attributes": [["identifier", "title", "type", "TEXT", "SORTABLE"]]}` | Attributes as nested dicts with `flags` key | `{"attributes": [{"identifier": "title", "type": "TEXT", "flags": ["SORTABLE"]}]}` |
| 2 | **FT.CONFIG GET** | Bytes keys/values → string keys/values | `dict` with bytes keys/values | `{b"TIMEOUT": b"500"}` | `dict` with string keys/values | `{"TIMEOUT": "500"}` |
| 3 | **FT.SEARCH** | New `warnings` attribute | `Result` with `.total`, `.docs` | `result.total == 2` | `Result` with new `.warnings` field | `result.warnings == []` |
| 4 | **FT.AGGREGATE** | New `total` and `warnings` attributes | `AggregateResult` with `.rows`, `.cursor` | `result.rows` | `AggregateResult` with `.total`, `.warnings` | `result.total == 5`, `result.warnings == []` |
| 5 | **FT.PROFILE** | Flat list profile data → dict (Redis ≥ 7.9.0) | Flat list profile data | `[result, ["Shards", [...]]]` | Tuple with `ProfileInformation` | `(result, ProfileInformation({"Shards": [...]}))` |

### Pipelines

| # | Component | Change | Previous Behavior | Previous Example | New Behavior | New Example |
|---|-----------|--------|------------------|------------------|--------------|-------------|
| 1 | **Module Pipelines** (JSON, TimeSeries, Search) | Pipeline callbacks now include core callbacks | Only module-specific callbacks available | Module pipeline missing core parsers | Full set of core + module callbacks available | All responses parsed consistently |

---

## Section 2: Users on RESP3 (protocol=3)

If you are using RESP3, the following response formats have changed. The most significant changes are that **Search module commands now return rich objects** and several commands gain proper parsing.

### Core Commands

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **ZINTER** (withscores) | Scores now normalized via `score_cast_func` | Identity lambda (no processing) | `[[b"a", 1.0]]` (no cast) | Scores processed through `score_cast_func` | `[[b"a", 1.0]]` (cast applied) |
| 2 | **ZPOPMAX/ZPOPMIN** | Scores normalized via callback | Identity lambda (no processing) | `[[b"a", 3.0]]` (no cast) | `list[list[val, float(score)]]` | `[[b"a", 3.0]]` (cast applied) |
| 3 | **ZRANDMEMBER** (withscores) | Scores cast to float | Native doubles (already float) | `[[b"a", 1.0]]` | Explicitly cast to `float` for consistency | `[[b"a", 1.0]]` |
| 4 | **BZPOPMAX/BZPOPMIN** | Score explicitly cast to float | `list[key, member, score]` | `[b"key", b"member", 3.0]` | `list[key, member, float]` | `[b"key", b"member", 3.0]` |
| 5 | **ACL CAT** | `bytes` → `str` | `list[bytes]` | `[b"read", b"write"]` | `list[str]` | `["read", "write"]` |
| 6 | **ACL GENPASS** | `bytes` → `str` | `bytes` | `b"abc123..."` | `str` | `"abc123..."` |
| 7 | **ACL HELP** | `bytes` → `str` | `list[bytes]` | `[b"ACL <subcommand>", ...]` | `list[str]` | `["ACL <subcommand>", ...]` |
| 8 | **ACL LIST** | `bytes` → `str` | `list[bytes]` | `[b"user default ...", ...]` | `list[str]` | `["user default ...", ...]` |
| 9 | **ACL USERS** | `bytes` → `str` | `list[bytes]` | `[b"default", b"admin"]` | `list[str]` | `["default", "admin"]` |
| 10 | **ACL WHOAMI** | `bytes` → `str` | `bytes` | `b"default"` | `str` | `"default"` |
| 11 | **CLIENT GETNAME** | `bytes` → `str` | `bytes` | `b"myconn"` | `str` | `"myconn"` |
| 12 | **GEOHASH** | `bytes` → `str` | `list[bytes]` | `[b"sqc8b49rny0"]` | `list[str]` | `["sqc8b49rny0"]` |
| 13 | **RESET** | `bytes` → `str` | `bytes` | `b"RESET"` | `str` | `"RESET"` |
| 14 | **STRALGO** (IDX) | Bytes keys → string keys | `dict` with bytes keys | `{b"matches": [...], b"len": 3}` | `dict` with string keys | `{"matches": [...], "len": 3}` |
| 15 | **SENTINEL MASTERS** | `list[dict]` → `dict[name → state_dict]` | `list[dict]` | `[{"name": "mymaster", ...}]` | `dict[name, dict]` | `{"mymaster": {"name": "mymaster", ...}}` |
| 16 | **ACL LOG** | `age-seconds` str → float; `client-info` str → dict | `dict` with string values | `{"age-seconds": "0.5", "client-info": "..."}` | `dict` with native types | `{"age-seconds": 0.5, "client-info": {"key": "val"}}` |
| 17 | **DEBUG OBJECT** | Raw string → parsed dict | Raw `bytes` string | `b"Value at:0x... refcount:1 ..."` | Parsed `dict` | `{"Value": "0x...", "refcount": 1, ...}` |
| 18 | **BGREWRITEAOF** | Status string → bool | Status `str` | `"Background append only file rewriting started"` | `bool` | `True` |
| 19 | **BGSAVE** | Status string → bool | Status `str` | `"Background saving started"` | `bool` | `True` |
| 20 | **CLUSTER LINKS** | Bytes keys → string keys | `list[dict]` with bytes keys | `[{b"direction": b"to", ...}]` | `list[dict]` with string keys | `[{"direction": b"to", ...}]` |
| 21 | **CLUSTER SHARDS** | Bytes keys → string keys at all levels | `dict` with bytes keys | `{b"slots": [...]}` | `dict` with string keys | `{"slots": [...]}` |

### Probabilistic Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **BF.INFO** | Raw dict → `BFInfo` object | `dict` with bytes keys | `{b"Capacity": 100, ...}` | `BFInfo` object | `bf_info.capacity == 100` |
| 2 | **CF.INFO** | Raw dict → `CFInfo` object | `dict` with bytes keys | `{b"Size": 512, ...}` | `CFInfo` object | `cf_info.size == 512` |
| 3 | **CMS.INFO** | Raw dict → `CMSInfo` object | `dict` with bytes keys | `{b"width": 2000, ...}` | `CMSInfo` object | `cms_info.width == 2000` |
| 4 | **TOPK.INFO** | Raw dict → `TopKInfo` object | `dict` with bytes keys | `{b"k": 5, ...}` | `TopKInfo` object | `topk_info.k == 5` |
| 5 | **TDIGEST.INFO** | Raw dict → `TDigestInfo` object | `dict` with bytes keys | `{b"Compression": 100, ...}` | `TDigestInfo` object | `tdigest_info.compression == 100` |
| 6 | **TDIGEST.BYRANK** | Raw list → parsed list with numeric coercion | Raw `list` | `[0.5, b"inf"]` | Parsed `list` with coerced values | `[0.5, "inf"]` |
| 7 | **TDIGEST.BYREVRANK** | Raw list → parsed list | Raw `list` | `[b"inf", 0.5]` | Parsed `list` with coerced values | `["inf", 0.5]` |
| 8 | **TDIGEST.CDF** | Raw list → parsed list | Raw `list` | `[0.5]` | Parsed `list` | `[0.5]` |
| 9 | **TDIGEST.QUANTILE** | Raw list → parsed list | Raw `list` | `[0.5]` | Parsed `list` | `[0.5]` |

### TimeSeries Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **TS.INFO** | Raw dict → `TSInfo` object | `dict` with bytes keys | `{b"totalSamples": 100, ...}` | `TSInfo` object | `ts_info.total_samples == 100` |

### JSON Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **JSON.TYPE** (non-existing key) | Wrapped None → bare None | `list` wrapping `None` | `[None]` | Bare `None` | `None` |

### Search (FT) Module

| # | Command | Change | Previous Response Format | Previous Example | New Response Format | New Example |
|---|---------|--------|------------------------|------------------|-------------------|-------------|
| 1 | **FT.SEARCH** | Raw dict → rich `Result` object | Raw `dict` | `{"total_results": 2, "results": [{"id": "doc1", "extra_attributes": {...}}]}` | `Result` object | `result.total == 2`, `result.docs[0].id == "doc1"`, `result.warnings == []` |
| 2 | **FT.AGGREGATE** | Raw dict → rich `AggregateResult` object | Raw `dict` | `{"total_results": 5, "results": [...], "warning": []}` | `AggregateResult` object | `result.total == 5`, `result.rows == [...]`, `result.warnings == []` |
| 3 | **FT.PROFILE** | Single wrapper → structured tuple | `ProfileInformation` wrapping entire response | `ProfileInformation(raw_dict)` | Tuple: `(Result\|AggregateResult, ProfileInformation)` | `result, profile = r.ft().profile(...)` |
| 4 | **FT.SPELLCHECK** | Raw nested structure → normalized dict | Raw nested dict/arrays | `{"results": {"importnt": [{"important": 0.0}]}}` | Normalized `dict[term, list[dict]]` | `{"importnt": [{"score": "0", "suggestion": "important"}]}` |
| 5 | **FT.INFO** | Bytes keys/values → string keys/values | `dict` with bytes keys/values | `{b"index_name": b"idx", ...}` | `dict` with string keys/values | `{"index_name": "idx", ...}` |
| 6 | **FT.CONFIG GET** | Bytes keys/values → string keys/values | `dict` with bytes keys/values | `{b"TIMEOUT": b"500"}` | `dict` with string keys/values | `{"TIMEOUT": "500"}` |
| 7 | **FT.SYNDUMP** | Bytes keys → string keys | `dict` with bytes keys | `{b"term": [b"syn1"]}` | `dict` with string keys | `{"term": ["syn1"]}` |
| 8 | **FT.HYBRID** | Raw dict → rich `HybridResult` object | Raw `dict` | `{"total_results": 1, ...}` | `HybridResult` object | `result.total_results == 1`, `result.warnings == []` |

### Pipelines

| # | Component | Change | Previous Behavior | Previous Example | New Behavior | New Example |
|---|-----------|--------|------------------|------------------|--------------|-------------|
| 1 | **Module Pipelines** (JSON, TimeSeries, Search) | Pipeline results now post-processed | Raw protocol data in pipeline results | Search commands returned raw dicts | `Result`/`AggregateResult`/`HybridResult` objects | Matches non-pipeline behavior |