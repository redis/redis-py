# RESP2 vs RESP3 Command Response Analysis — Unified in redis-py 8.0+

This document lists commands where the Python return object **previously** differed between RESP2 and RESP3 protocols, and documents how each has been **unified** in redis-py 8.0+.

> ⚠️ **All changes listed below are breaking changes** for existing users.

**Sources analyzed:**
- `redis/_parsers/helpers.py` — `_RedisCallbacks`, `_RedisCallbacksRESP2`, `_RedisCallbacksRESP3`
- `redis/commands/core.py` — command definitions and type hints
- Module callback registrations (`bf`, `json`, `search`, `timeseries`, `vectorset`)
- Redis official documentation (redis.io)

**Note on `decode_responses`:** All differences below assume `decode_responses=False` (the default). With `decode_responses=True`, some `str` vs `bytes` differences disappear.

---

## Part 1: Core Commands — Previously Different, Now Unified

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | ZDIFF (withscores) | `list[tuple(val, score)]` — scores as bytes | Raw `list[list]` — scores as float | `list[list[val, float]]` | ✅ RESP2 callback returns `list[list]` with `float(score)` before `score_cast_func` |
| 2 | ZINTER (withscores) | `list[tuple(val, score)]` — scores as bytes | Raw `list[list]` — identity passthrough | `list[list[val, float]]` | ✅ Both protocols produce `list[list]` with `float(score)` |
| 3 | ZPOPMAX | `list[tuple(val, score)]` | Raw `list` — identity passthrough | `list[list[val, float]]` | ✅ Unified callback produces nested lists with float scores |
| 4 | ZPOPMIN | `list[tuple(val, score)]` | Raw `list` — identity passthrough | `list[list[val, float]]` | ✅ Same as ZPOPMAX |
| 5 | ZRANGE (withscores) | `list[tuple(val, score)]` — scores as bytes | `list[list[val, score]]` — scores as float | `list[list[val, float]]` | ✅ RESP2 now returns `list[list]`; scores cast to `float()` in both |
| 6 | ZRANGEBYSCORE (withscores) | `list[tuple(val, score)]` — scores as bytes | `list[list[val, score]]` — scores as float | `list[list[val, float]]` | ✅ Same; `score_cast_func` receives `float` in both protocols |
| 7 | ZREVRANGE (withscores) | `list[tuple(val, score)]` — scores as bytes | `list[list[val, score]]` — scores as float | `list[list[val, float]]` | ✅ Same |
| 8 | ZREVRANGEBYSCORE (withscores) | `list[tuple(val, score)]` — scores as bytes | `list[list[val, score]]` — scores as float | `list[list[val, float]]` | ✅ Same |
| 9 | ZUNION (withscores) | `list[tuple(val, score)]` — scores as bytes | `list[list[val, score]]` — scores as float | `list[list[val, float]]` | ✅ Same |
| 10 | ZRANDMEMBER (withscores) | Flat interleaved `list` (len=2N) | Nested `list[list]` (len=N) | `list[list[val, float]]` | ✅ RESP2 flat list paired into nested lists; scores cast to float |
| 11 | ZRANK/ZREVRANK (withscore) | `[int, int]` | `[int, float]` | `[int, float]` | ✅ Score cast to `float()` before `score_cast_func` |
| 12 | ZSCAN | Scores passed as `bytes` to `score_cast_func` | Scores passed as `float` to `score_cast_func` | Scores passed as `float` to `score_cast_func` | ✅ `float(score)` applied before `score_cast_func` in both protocols |
| 13 | ZMPOP | Scores as `b"1"` bytes strings | Scores as `1.0` native doubles | `[key, list[list[val, float]]]` | ✅ Scores normalized to float; structure unified to nested lists |
| 14 | BZMPOP | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP | ✅ Same as ZMPOP |
| 15 | HRANDFIELD (withvalues) | Flat interleaved `list` (len=2N) | Nested `list[list]` (len=N) | `list[list[field, value]]` | ✅ RESP2 flat list paired into nested lists |
| 16 | BLPOP | `tuple(key, val)` | `list[key, val]` | `list[key, val]` | ✅ RESP2 callback returns `list` instead of `tuple` |
| 17 | BRPOP | `tuple(key, val)` | `list[key, val]` | `list[key, val]` | ✅ Same as BLPOP |
| 18 | BZPOPMAX | `tuple(key, member, float(score))` | `list[key, member, score]` | `list[key, member, float]` | ✅ RESP2 returns `list`; score cast to `float` in both |
| 19 | BZPOPMIN | `tuple(key, member, float(score))` | `list[key, member, score]` | `list[key, member, float]` | ✅ Same as BZPOPMAX |
| 20 | LCS (with IDX) | Flat `list` `[b"matches", [...], b"len", 6]` | Native `dict` `{b"matches": [...], b"len": 6}` | `dict` with string keys `{"matches": [...], "len": 6}` | ✅ RESP2 flat list parsed to dict; keys normalized to string |
| 21 | ACL CAT | `list[str]` (decoded via `str_if_bytes`) | Raw `list[bytes]` | `list[str]` | ✅ RESP3 callback added: `str_if_bytes` applied |
| 22 | ACL GENPASS | `str` (via `str_if_bytes`) | Raw `bytes` | `str` | ✅ RESP3 callback added |
| 23 | ACL GETUSER | `dict` with selectors as `list[list[str]]` | `dict` with selectors as `list[dict[str,str]]` | `dict` with selectors as `list[dict[str,str]]` | ✅ RESP2 flat selector lists converted to dicts |
| 24 | ACL HELP | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[str]` | ✅ RESP3 callback added |
| 25 | ACL LIST | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[str]` | ✅ RESP3 callback added |
| 26 | ACL LOG | `list[dict]` (age-seconds=`float`, client-info=`dict`) | `list[dict]` (age-seconds=`str`, client-info=`str`) | `list[dict]` (age-seconds=`float`, client-info=`dict`) | ✅ RESP3 callback now parses `age-seconds` to float and `client-info` to dict |
| 27 | ACL USERS | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[str]` | ✅ RESP3 callback added |
| 28 | ACL WHOAMI | `str` (via `str_if_bytes`) | Raw `bytes` | `str` | ✅ RESP3 callback added |
| 29 | CLIENT GETNAME | `str` (via `str_if_bytes`) | Raw `bytes` | `str` | ✅ RESP3 callback added |
| 30 | CLIENT TRACKINGINFO | Flat `list[str]` | Raw `dict` with bytes keys | `dict` with string keys | ✅ RESP2 flat list parsed to dict; RESP3 keys normalized to string |
| 31 | CLUSTER GETKEYSINSLOT | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[bytes]` | ✅ RESP2 callback removed — returns raw `bytes` to match RESP3 |
| 32 | COMMAND | `dict` (flags=`list[str]`) | `dict` (flags=`set[str]`, +`acl_categories`) | `dict` (flags=`set[str]`, +`acl_categories`) | ✅ RESP2 callback updated to produce `set` flags and include `acl_categories` |
| 33 | COMMAND GETKEYS | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[bytes]` | ✅ RESP2 callback removed — returns raw `bytes` to match RESP3 |
| 34 | BGREWRITEAOF | `True` (bool, always) | Raw status string | `bool` (`True`) | ✅ RESP3 callback added: returns `True` |
| 35 | BGSAVE | `True` (bool, always) | Raw status string | `bool` (`True`) | ✅ RESP3 callback added: returns `True` |
| 36 | DEBUG OBJECT | Parsed `dict` | Raw `str`/`bytes` | Parsed `dict` | ✅ RESP3 callback added: `parse_debug_object` applied |
| 37 | GEOHASH | `list[str]` (via `str_if_bytes`) | Raw `list[bytes]` | `list[str]` | ✅ RESP3 callback added |
| 38 | GEOPOS | `list[tuple(float,float)\|None]` | Raw `list[list[float]\|None]` | `list[list[float,float]\|None]` | ✅ RESP2 returns `list` instead of `tuple` for coordinates |
| 39 | MEMORY STATS | `dict` (str keys, str-decoded values) | `dict` (str keys, native int values) | `dict` (str keys, native int values) | ✅ RESP2 callback updated to preserve native types |
| 40 | RESET | `str` (via `str_if_bytes`) | Raw `bytes` | `str` | ✅ RESP3 callback added |
| 41 | SENTINEL MASTER | `dict` (flags=comma-separated `str`) | `dict` (flags=`set`) | `dict` (flags=`set`) | ✅ RESP2 callback splits comma-separated flags into `set` |
| 42 | SENTINEL MASTERS | `dict[name→state_dict]` | `list[dict]` | `dict[name→state_dict]` (flags=`set`) | ✅ RESP3 callback converts `list[dict]` to `dict[name→state]`; flags as `set` in both |
| 43 | SENTINEL SENTINELS | `list[dict]` (flags=comma `str`) | `list[dict]` (flags=`set`) | `list[dict]` (flags=`set`) | ✅ RESP2 callback splits comma-separated flags into `set` |
| 44 | SENTINEL SLAVES | `list[dict]` (flags=comma `str`) | `list[dict]` (flags=`set`) | `list[dict]` (flags=`set`) | ✅ Same as SENTINEL SENTINELS |
| 45 | STRALGO (IDX) | `dict` (positions as `tuple`) | `dict` (positions as `list`, bytes keys) | `dict` (positions as `list`, string keys) | ✅ RESP2 returns `list` for positions; RESP3 keys normalized to string via `parse_stralgo_resp3` |
| 46 | XINFO CONSUMERS | `list[dict]` (str keys, bytes values) | `list[dict]` (str keys, native int values) | `list[dict]` (str keys, native types) | ✅ RESP2 callback updated to parse native types |
| 47 | XINFO GROUPS | `list[dict]` (str keys, bytes values) | `list[dict]` (str keys, native int values) | `list[dict]` (str keys, native types) | ✅ Same as XINFO CONSUMERS |
| 48 | XREAD | `list[list]` (empty: `[]`) | `dict` (empty: `{}`) | `dict[stream, entries]` (empty: `{}`) | ✅ RESP2 callback converts `list[list]` to `dict` |
| 49 | XREADGROUP | `list[list]` | `dict` | `dict[stream, entries]` | ✅ Same as XREAD |
| 50 | CLUSTER LINKS | Flat `list[list]` | `list[dict]` with bytes keys | `list[dict]` with string keys | ✅ Unified `parse_cluster_links()` normalizes both protocols |
| 51 | CLUSTER SHARDS | `dict` with bytes keys at node level | `dict` with bytes keys at all levels | `dict` with string keys at all levels | ✅ `parse_cluster_shards()` normalizes all keys to strings |
| 52 | FUNCTION LIST | Flat key-value sublists | Nested `list[dict]` (native maps) | `list[dict]` with nested dicts | ✅ RESP2 `parse_function_list()` converts flat lists to nested dicts |

---

## Part 2: Module Commands — Previously Different, Now Unified

Module commands previously registered RESP2-only callbacks while RESP3 returned raw server responses. All modules below now have unified callbacks for both protocols.

### 2.1 Probabilistic Module (BF, CF, CMS, TopK, TDigest)

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | BF.INFO | `BFInfo` object | Raw `dict` with bytes keys | `BFInfo` object | ✅ RESP3 callback added: raw dict parsed into `BFInfo` |
| 2 | CF.INFO | `CFInfo` object | Raw `dict` with bytes keys | `CFInfo` object | ✅ RESP3 callback added: raw dict parsed into `CFInfo` |
| 3 | CMS.INFO | `CMSInfo` object | Raw `dict` with bytes keys | `CMSInfo` object | ✅ RESP3 callback added: raw dict parsed into `CMSInfo` |
| 4 | TOPK.INFO | `TopKInfo` object | Raw `dict` with bytes keys | `TopKInfo` object | ✅ RESP3 callback added: raw dict parsed into `TopKInfo` |
| 5 | TOPK.ADD | `list` (with incorrect int/float coercion) | Raw response | `list` (numeric strings preserved as-is) | ✅ Removed incorrect coercion from RESP2 callback |
| 6 | TOPK.INCRBY | `list` (with incorrect int/float coercion) | Raw response | `list` (numeric strings preserved as-is) | ✅ Same as TOPK.ADD |
| 7 | TOPK.LIST | `list` (with incorrect int/float coercion) | Raw response | `list` (numeric strings preserved as-is) | ✅ Same as TOPK.ADD |
| 8 | TDIGEST.INFO | `TDigestInfo` object | Raw `dict` with bytes keys | `TDigestInfo` object | ✅ RESP3 callback added: raw dict parsed into `TDigestInfo` |
| 9 | TDIGEST.BYRANK | `list` | Raw `list` | `list` (with numeric coercion) | ✅ RESP3 callback added |
| 10 | TDIGEST.BYREVRANK | `list` | Raw `list` | `list` (with numeric coercion) | ✅ RESP3 callback added |
| 11 | TDIGEST.CDF | `list` | Raw `list` | `list` | ✅ RESP3 callback added |
| 12 | TDIGEST.QUANTILE | `list` | Raw `list` | `list` | ✅ RESP3 callback added |

### 2.2 TimeSeries (TS) Module

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | TS.GET | `tuple(timestamp, value)` | Raw response | `list[timestamp, value]` | ✅ Both protocols return `list`; RESP2 changed from `tuple` |
| 2 | TS.INFO | `TSInfo` object | Raw `dict` with bytes keys | `TSInfo` object | ✅ RESP3 callback added: raw dict parsed into `TSInfo` |
| 3 | TS.MGET | Sorted `list[dict]` | Raw response | `dict[key, [labels, sample]]` | ✅ Unified parser for both protocols |
| 4 | TS.MRANGE | Sorted `list[dict]` (2-element value) | Raw response | `dict` (3-element value: labels, metadata, samples) | ✅ Unified parser; metadata slot added |
| 5 | TS.MREVRANGE | Same as TS.MRANGE | Raw response | Same as TS.MRANGE | ✅ Same as TS.MRANGE |
| 6 | TS.RANGE | `list[tuple(ts, val)]` | Raw response | `list[list[ts, val]]` | ✅ Both protocols return `list[list]`; RESP2 changed from `list[tuple]` |
| 7 | TS.REVRANGE | `list[tuple(ts, val)]` | Raw response | `list[list[ts, val]]` | ✅ Same as TS.RANGE |
| 8 | TS.QUERYINDEX | `list` (with incorrect int coercion) | Raw response | `list` (key strings preserved as-is) | ✅ Removed incorrect int coercion |

### 2.3 JSON Module

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | JSON.NUMINCRBY | Scalar value (for legacy paths) | Array-wrapped value | Array-wrapped value `[N]` | ✅ RESP2 normalized to array for legacy paths |
| 2 | JSON.NUMMULTBY | Scalar value (for legacy paths) | Array-wrapped value | Array-wrapped value `[N]` | ✅ Same as JSON.NUMINCRBY |
| 3 | JSON.RESP | String-encoded floats in nested lists | Native floats in nested lists | Native Python floats in nested lists | ✅ `_convert_resp_floats()` normalizes RESP2 string floats |
| 4 | JSON.OBJKEYS | Keys forced to `str` via `nativestr` | Keys as `bytes` (raw) | Keys respect `decode_responses` setting | ✅ Removed `nativestr`; keys are `bytes` when `decode_responses=False` |
| 5 | JSON.TYPE (non-existing key) | `None` | `[None]` (wrapped) | `None` | ✅ RESP3 callback unwraps `[None]` to bare `None` |
| 6 | JSON.ARRAPPEND | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed — already effectively unified via `_decode` |
| 7 | JSON.ARRINDEX | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 8 | JSON.ARRINSERT | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 9 | JSON.ARRLEN | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 10 | JSON.ARRTRIM | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 11 | JSON.STRAPPEND | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 12 | JSON.OBJLEN | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |
| 13 | JSON.STRLEN | `self._decode` decoded | Raw response | Decoded response | ⚠️ Not changed |

### 2.4 Search (FT) Module

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | FT.SEARCH | `Result` object | Raw `dict` | `Result` object (with new `.warnings` field) | ✅ RESP3 `_parse_search_resp3()` builds `Result` from raw dict; `.from_resp3()` classmethod added |
| 2 | FT.AGGREGATE | `AggregateResult` object | Raw `dict` | `AggregateResult` object (with `.total`, `.warnings`) | ✅ RESP3 `_parse_aggregate_resp3()` builds `AggregateResult` from raw dict |
| 3 | FT._HYBRID | Parsed result | Raw `dict` | `HybridResult` object | ✅ RESP3 `_parse_hybrid_search_resp3()` builds `HybridResult` |
| 4 | FT.PROFILE | Parsed tuple | Raw response | `(Result\|AggregateResult, ProfileInformation)` tuple | ✅ RESP3 `_parse_profile_resp3()` added; RESP2 updated for >= 7.9.0 format |
| 5 | FT.SPELLCHECK | Parsed `dict` | Raw nested structure | Normalized `dict[term, list[dict]]` | ✅ RESP3 `_parse_spellcheck_resp3()` added |
| 6 | FT.INFO | Parsed `dict` (with flat attribute sublists) | Raw `dict` with bytes keys | `dict` with string keys, structured attribute dicts | ✅ RESP2 attributes normalized to dicts with `flags` key; RESP3 `_parse_info_resp3()` normalizes keys to strings |
| 7 | FT.CONFIG GET | Parsed `dict` with bytes keys/values | Raw `dict` with bytes keys/values | `dict` with string keys/values | ✅ Both protocols normalize to string keys/values via `to_string()` |
| 8 | FT.SYNDUMP | Parsed `dict` | Raw `dict` with bytes keys | `dict` with string keys | ✅ RESP3 `_parse_syndump_resp3()` normalizes keys to strings |

### 2.5 VectorSet Module (not changed)

| # | Command | Previous RESP2 Return Type | Previous RESP3 Return Type | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | VINFO | `dict` (flat list→dict) | Raw `dict` from server | `dict` | ⚠️ Already effectively unified — both produce `dict` |
| 2 | VLINKS | Parsed result | Raw response | — | ⚠️ Not changed in this round |

---

## Part 3: Commands Previously with No Callback but Different Raw Responses — Now Unified

These commands previously had no registered callback and returned raw server responses that differed by protocol. Now unified:

| # | Command | Previous RESP2 Raw Response | Previous RESP3 Raw Response | Unified Return Type (8.0+) | Resolution |
|---|---------|---------------------------|---------------------------|---------------------------|------------|
| 1 | ZMPOP | `list\|None` (scores as `b"1"` bytes) | `list\|None` (scores as `1.0` float) | `[key, list[list[val, float]]]\|None` | ✅ Unified callback normalizes scores to float and structure to nested lists |
| 2 | BZMPOP | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP | ✅ Same as ZMPOP |
| 3 | LCS (with IDX) | Flat `list` `[b"matches", [...], b"len", 6]` | Native `dict` `{b"matches": [...], b"len": 6}` | `dict` with string keys | ✅ RESP2 flat list parsed to dict; keys normalized to string in both |

---

## Part 4: Summary — Unified State in redis-py 8.0+

### 4.1 Protocol-Level Differences (handled transparently by redis-py 8.0+)

| Category | RESP2 Wire Format | RESP3 Wire Format | redis-py 8.0+ Unified Behavior |
|----------|------------------|------------------|-------------------------------|
| Maps/Dicts | Flat arrays `[k1, v1, k2, v2]` | Native map type | Both produce Python `dict` |
| Sets | Arrays | Native set type | Both produce Python `set` (where applicable) |
| Doubles | Bulk strings `b"3.14"` | Native double type | Both produce Python `float` |
| Booleans | Integers 0/1 | Native boolean type | Both produce Python `bool` |
| Nulls | `$-1` (nil bulk) / `*-1` (nil array) | Null type | Both produce `None` |

### 4.2 Breaking Changes by Category (RESP2 users)

| # | Category | Commands Affected | Previous RESP2 Behavior | New Unified Behavior | Migration Impact |
|---|----------|------------------|------------------------|---------------------|-----------------|
| 1 | **tuple → list** | BLPOP, BRPOP, BZPOPMAX, BZPOPMIN, GEOPOS | Returned `tuple` | Returns `list` | `isinstance(r, tuple)` checks break; unpacking still works |
| 2 | **tuple → list (zset)** | ZDIFF, ZINTER, ZPOPMAX, ZPOPMIN, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZUNION (withscores) | `list[tuple(val, score)]` | `list[list[val, float]]` | Type checks differ; unpacking still works |
| 3 | **flat → nested** | ZRANDMEMBER (withscores), HRANDFIELD (withvalues) | Flat interleaved list (len=2N) | Nested `list[list]` (len=N) | `len()` and iteration patterns change |
| 4 | **score normalization** | All ZSET withscores, ZMPOP, BZMPOP, ZRANK, ZSCAN | Scores as bytes `b"1"` | Scores as `float` `1.0` | Arithmetic works directly; `score_cast_func` receives `float` |
| 5 | **list → dict (streams)** | XREAD, XREADGROUP | `[[stream, entries]]` | `{stream: [entries]}` | Access pattern changes from indexing to key lookup |
| 6 | **flat → dict** | LCS (with IDX), FUNCTION LIST, CLIENT TRACKINGINFO | Flat lists | `dict` / `list[dict]` | Key-based access instead of positional |
| 7 | **flags → set** | SENTINEL MASTER/SENTINELS/SLAVES, COMMAND | Comma-separated `str` / `list[str]` | `set` | `"master,odown"` → `{"master", "odown"}` |
| 8 | **Info objects (modules)** | BF/CF/CMS/TOPK/TDIGEST.INFO, TS.INFO | Already `Info` objects | Still `Info` objects (no change for RESP2) | No impact |
| 9 | **Search rich objects** | FT.SEARCH, FT.AGGREGATE, FT._HYBRID, FT.PROFILE | Already `Result`/`AggregateResult` | Now includes `.warnings`, `.total` fields | Minor — new fields added |
| 10 | **JSON normalization** | JSON.RESP, JSON.NUMINCRBY/NUMMULTBY | String floats, scalar results | Native floats, array-wrapped results | `json.resp()` returns native floats; `numincrby()` returns `[N]` |

### 4.3 Breaking Changes by Category (RESP3 users)

| # | Category | Commands Affected | Previous RESP3 Behavior | New Unified Behavior | Migration Impact |
|---|----------|------------------|------------------------|---------------------|-----------------|
| 1 | **bytes → str** | ACL CAT/LIST/USERS/WHOAMI/GENPASS/HELP, CLIENT GETNAME, GEOHASH, RESET | Raw `bytes` | `str` (via `str_if_bytes`) | String comparison code simplifies |
| 2 | **raw dict → rich object** | BF/CF/CMS/TOPK/TDIGEST.INFO, TS.INFO | Raw `dict` with bytes keys | `Info` objects (attribute access) | `d[b"Capacity"]` → `obj.capacity` |
| 3 | **raw dict → Result** | FT.SEARCH, FT.AGGREGATE, FT._HYBRID, FT.PROFILE | Raw `dict` / `list` | `Result`, `AggregateResult`, `HybridResult`, `ProfileInformation` | Complete access pattern change |
| 4 | **bytes keys → str keys** | FT.INFO, FT.CONFIG GET, FT.SYNDUMP, CLUSTER LINKS/SHARDS | `dict` with `bytes` keys | `dict` with `str` keys | Key access changes from `d[b"key"]` to `d["key"]` |
| 5 | **raw → parsed** | BGREWRITEAOF, BGSAVE, DEBUG OBJECT | Raw status strings | `bool(True)` / parsed `dict` | Access pattern changes |
| 6 | **score normalization** | ZSET withscores commands | Already `float` (no change) | Still `float` | No impact |
| 7 | **JSON unwrap** | JSON.TYPE (non-existing key) | `[None]` (wrapped) | `None` (unwrapped) | Check for `None` instead of `[None]` |
| 8 | **Pipeline post-processing** | All module pipeline commands | Raw responses (no callbacks) | Full callback processing | Pipeline results now match non-pipeline results |

### 4.4 Total Unified Command Count

| Section | Count | Description |
|---------|-------|-------------|
| Part 1: Core commands | 52 | Commands with explicit callback unification |
| Part 2: Module commands | 41 | Module commands with unified RESP2/RESP3 callbacks |
| Part 3: Previously raw commands | 3 | Commands that previously had no callbacks, now unified |
| **Total** | **~96** | **Commands unified across RESP2 and RESP3 in redis-py 8.0+** |

### 4.5 Pipeline Unification

In addition to individual command callbacks, **pipeline response processing** was unified:
- **Core pipelines**: Already had callback propagation
- **JSON pipelines**: Fixed to inherit full `response_callbacks` from core + JSON modules
- **Search pipelines**: Fixed to inherit full `response_callbacks` from core + Search modules
- **TimeSeries pipelines**: Fixed to inherit full `response_callbacks` from core + TimeSeries modules

This ensures that pipeline results are post-processed identically to non-pipeline results in both protocols.
