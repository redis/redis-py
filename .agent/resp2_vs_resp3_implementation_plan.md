# RESP2 vs RESP3 Unification — Implementation Plan

**Goal:** Unify the Python return types across RESP2 and RESP3 so that user code gets the same object type regardless of protocol version.

**Rule:** Prefer the RESP3-closest format as the unified "Final Type" (e.g., `list` over `tuple`, `set` over comma-separated string, native `dict` over flat list). Exception: where a semantic type is clearly more useful (e.g., `bool` for background ops).

**Action:** For each command, the RESP2 callback is adjusted to produce the Final Type. In some cases, a RESP3 callback is also added/adjusted to normalize raw server responses. All existing unit tests must be updated to expect the new unified Final Type — replace `assert_resp_response` (dual-assertion) calls with a single assertion matching the Final Type, and fix any other test assertions that relied on the old protocol-specific return values.

**Return type hints:** For every batch, update the corresponding type aliases in `redis/typing.py` and any return type annotations in `redis/commands/core.py` (including `@overload` signatures) to match the new unified Final Type. Remove dead union branches that referred to the old protocol-specific format (e.g., `list[tuple[...]]` branches after unifying to `list[list[...]]`).

---

## Batch 1: Sorted Set Score Pairs — `tuple` → `list`

**Theme:** RESP2 uses `zset_score_pairs` which returns `list[tuple(val, score)]`. RESP3 returns `list[list[val, score]]`. Unify to `list[list]`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | ZDIFF (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Change RESP2 callback from `zset_score_pairs` → `zset_score_pairs_resp3` (or equivalent producing lists) |
| 2 | ZINTER (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` (identity λ) | `list[list[val,score]]` | Change RESP2 callback; add RESP3 callback `zset_score_pairs_resp3` to normalize score types |
| 3 | ZPOPMAX | `list[tuple(val,score)]` | raw `list` (identity λ) | `list[list[val,score]]` | Change RESP2 callback; add RESP3 callback to normalize structure |
| 4 | ZPOPMIN | `list[tuple(val,score)]` | raw `list` (identity λ) | `list[list[val,score]]` | Same as ZPOPMAX |
| 5 | ZRANGE (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Change RESP2 callback from `zset_score_pairs` → unified callback |
| 6 | ZRANGEBYSCORE (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Same |
| 7 | ZREVRANGE (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Same |
| 8 | ZREVRANGEBYSCORE (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Same |
| 9 | ZUNION (withscores) | `list[tuple(val,score)]` | `list[list[val,score]]` | `list[list[val,score]]` | Same |

**Implementation notes:**
- Replace `zset_score_pairs` (uses `zip` → tuples) with a list-based version for RESP2
- `zset_score_pairs_resp3` already returns `list[list]` — reuse or make shared
- Ensure `score_cast_func` is applied consistently in both paths

**Unit test fixes (`tests/test_commands.py`):**
- Replace all `assert_resp_response(r, ..., [(b"x", N)], [[b"x", N]])` with single `assert ... == [[b"x", N]]` for: `test_zdiff`, `test_zinter`, `test_zpopmax`, `test_zpopmin`, `test_zrange`, `test_zrangebyscore`, `test_zrevrange`, `test_zrevrangebyscore`, `test_zunion`, `test_zunionstore`, `test_zinterstore`
- ZPOPMAX/ZPOPMIN single-element: RESP2 currently `[(b"a3", 3)]`, RESP3 `[b"a3", 3.0]` — unify assertion to `[[b"a3", 3.0]]`
- `score_cast_func=str` tests: RESP2 gives `"2"`, RESP3 gives `"2.0"` — pick one and unify (prefer `"2.0"` for float-based cast consistency, or document the expected `str(float(x))` behavior)

---

## Batch 2: Flat vs Nested Score/Value Pairs (ZRANDMEMBER, HRANDFIELD)

**Theme:** RESP2 returns flat interleaved lists; RESP3 returns nested pairs. Unify to nested `list[list]`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | ZRANDMEMBER (withscores) | Flat `[val, score, val, score]` (len=2N) | Nested `[[val, score], ...]` (len=N) | `list[list[val, float(score)]]` | Add RESP2 callback to pair+convert flat list; add RESP3 callback to cast scores to float |
| 2 | HRANDFIELD (withvalues) | Flat `[field, val, field, val]` (len=2N) | Nested `[[field, val], ...]` (len=N) | `list[list[field, val]]` | Add RESP2 callback to pair flat list into nested; keep RESP3 as-is |

**Implementation notes:**
- ZRANDMEMBER scores in RESP2 come as bytes strings (`b"1"`), in RESP3 as native doubles (`1.0`) — normalize to `float`
- These commands currently have NO callback at all — need to register new ones

**Unit test fixes (`tests/test_commands.py`):**
- `test_hrandfield`: replace `assert_resp_response(r, len(r.hrandfield("key", 2, withvalues=True)), 4, 2)` with `assert len(...) == 2` (nested pairs)
- `test_zrandmember`: replace any `len()` assertions that expect flat length (2N) with nested length (N); replace flat list assertions with nested `[[val, score]]` format

---

## Batch 3: Blocking Pop Commands — `tuple` → `list`

**Theme:** RESP2 wraps results in `tuple()`, RESP3 returns raw `list`. Unify to `list`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | BLPOP | `tuple\|None` | `list\|None` | `list\|None` | Remove RESP2 `tuple()` wrapping — return raw list |
| 2 | BRPOP | `tuple\|None` | `list\|None` | `list\|None` | Same |
| 3 | BZPOPMAX | `tuple(key,member,float(score))\|None` | `list[key,member,score]` | `list[key,member,float(score)]\|None` | Change RESP2 to return `list` instead of `tuple`; add RESP3 callback to cast score to `float` |
| 4 | BZPOPMIN | `tuple(key,member,float(score))\|None` | `list[key,member,score]` | `list[key,member,float(score)]\|None` | Same as BZPOPMAX |

**Implementation notes:**
- BZPOPMAX/BZPOPMIN RESP2 does `float(r[2])` — keep this normalization but return `list` not `tuple`
- RESP3 score comes as native double — still apply `float()` for consistency, but only if the value is not already a `float` (guard with `isinstance` check to avoid redundant conversion)

**Unit test fixes (`tests/test_commands.py`):**
- `test_blpop`: replace `assert_resp_response(r, r.blpop(...), (b"b", b"3"), [b"b", b"3"])` → `assert r.blpop(...) == [b"b", b"3"]` (all 5 assertions)
- `test_brpop`: same pattern — replace 5 `assert_resp_response` with single `assert ... == [...]`
- `test_bzpopmax`: replace `assert_resp_response(r, r.bzpopmax(...), (b"b", b"b2", 20), [b"b", b"b2", 20])` → `assert r.bzpopmax(...) == [b"b", b"b2", 20.0]` (note: score as `float`; all 5 assertions)
- `test_bzpopmin`: same pattern as bzpopmax (5 assertions)

---

## Batch 4: Raw Protocol Differences (no callback) — ZMPOP, BZMPOP, LCS

**Theme:** Commands with no callback that have protocol-level type differences in raw response.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | ZMPOP | Scores as `b"1"` bytes; flat structure | Scores as `1.0` doubles; nested structure | Normalized: scores as `float`, nested `list[list[val,score]]` | Add callback for both protocols to normalize structure and scores |
| 2 | BZMPOP | Same as ZMPOP | Same as ZMPOP | Same as ZMPOP | Same |
| 3 | LCS (with IDX) | Flat `[b"matches",[...],b"len",6]` list | Native `{b"matches":[...],b"len":6}` dict | `dict` | Add RESP2 callback to convert flat list → dict |

**Unit test fixes (`tests/test_commands.py`):**
- `test_lcs`: replace `assert_resp_response(r, r.lcs("foo","bar",idx=True,...), <resp2_flat>, <resp3_dict>)` → single `assert ... == <dict_format>` (3 assertions around lines 1954-1970)
- ZMPOP/BZMPOP: find tests that compare raw scores as bytes vs float and unify to float+nested structure

---

## Batch 5: `str` vs `bytes` — Unify `str_if_bytes` handling

**Theme:** RESP2 callbacks apply `str_if_bytes` decoding bytes→str for certain commands, while RESP3 returns raw bytes. Two groups:

### Group A: Always-ASCII commands — Unify to `str` (apply `str_if_bytes` to RESP3 too)
These commands return Redis-internal strings that are guaranteed printable ASCII (category names, hex passwords, help text, usernames, client names, geohashes). Unify by adding `str_if_bytes` to RESP3 callbacks so both protocols return `str`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | ACL CAT | `list[str]` | `list[bytes]` | `list[str]` | Add RESP3 callback with `str_if_bytes` |
| 2 | ACL GENPASS | `str` | `bytes` | `str` | Add RESP3 callback with `str_if_bytes` |
| 3 | ACL HELP | `list[str]` | `list[bytes]` | `list[str]` | Add RESP3 callback with `str_if_bytes` |
| 4 | ACL LIST | `list[str]` | `list[bytes]` | `list[str]` | Add RESP3 callback with `str_if_bytes` |
| 5 | ACL USERS | `list[str]` | `list[bytes]` | `list[str]` | Add RESP3 callback with `str_if_bytes` |
| 6 | ACL WHOAMI | `str` | `bytes` | `str` | Add RESP3 callback with `str_if_bytes` |
| 7 | CLIENT GETNAME | `str` | `bytes` | `str` | Add RESP3 callback with `str_if_bytes` |
| 8 | GEOHASH | `list[str]` | `list[bytes]` | `list[str]` | Add RESP3 callback with `str_if_bytes` |
| 9 | RESET | `str` | `bytes` | `str` | Add RESP3 callback with `str_if_bytes` |

### Group B: User-data commands — Unify to `bytes` (remove `str_if_bytes` from RESP2)
These commands return actual key names which can be arbitrary binary data (non-UTF-8). Unify by removing `str_if_bytes` from RESP2 so both protocols return raw `bytes`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | CLUSTER GETKEYSINSLOT | `list[str]` | `list[bytes]` | `list[bytes]` | Remove RESP2 `str_if_bytes` mapping |
| 2 | COMMAND GETKEYS | `list[str]` | `list[bytes]` | `list[bytes]` | Remove RESP2 `str_if_bytes` mapping |

**Implementation notes:**
- Group A: Keep existing RESP2 callbacks, add matching `str_if_bytes` callbacks in `_RedisCallbacksRESP3`
- Group B: Remove the `str_if_bytes` mappings from `_RedisCallbacksRESP2` for these two commands
- With `decode_responses=True`, both protocols already return `str` after the decoder layer for all commands
- ⚠️ Group B is a **breaking change** for RESP2 users who relied on `str` without `decode_responses=True`

**Unit test fixes (`tests/test_commands.py`):**
- Group A tests: replace `assert_resp_response(r, ..., "str_val", b"bytes_val")` → `assert ... == "str_val"` (unified to str)
- `test_client_getname`: `assert r.client_getname() == "redis_py_test"`
- `test_reset`: `assert r.reset() == "RESET"`
- `test_geohash`: replace any `assert_resp_response` with `assert ... == ["..."]`
- ACL tests: replace dual assertions with str-only assertions
- Group B tests: replace `assert_resp_response(r, ..., ["str"], [b"bytes"])` → `assert ... == [b"bytes"]` (unified to bytes)
- `test_command_getkeys`: `assert res == [b"a", b"c", b"e"]`
- `test_cluster_getkeysinslot` (in cluster tests): replace string assertions with bytes

---

## Batch 6: Structural Differences — Streams, Tracking, Geo

**Theme:** Commands where RESP2 and RESP3 return fundamentally different structures. Unify to the RESP3 native format.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | XREAD | `list[list[stream, entries]]` (empty: `[]`) | `dict{stream: [entries]}` (empty: `{}`) | `dict{stream: [entries]}` (empty: `{}`) | Change RESP2 `parse_xread` to return dict |
| 2 | XREADGROUP | `list[list[stream, entries]]` | `dict{stream: [entries]}` | `dict{stream: [entries]}` | Same as XREAD |
| 3 | CLIENT TRACKINGINFO | `list[str]` (flat) | `dict` (native map) | `dict` | Change RESP2 to parse flat list into dict; add RESP3 normalization |
| 4 | GEOPOS | `list[tuple(float,float)\|None]` | `list[list[float,float]\|None]` | `list[list[float,float]\|None]` | Change RESP2 callback to return `list` instead of `tuple` for coordinates |
| 5 | STRALGO (IDX) | `dict` (positions as `tuple`) | `dict` (positions as `list`) | `dict` (positions as `list`) | Change RESP2 `parse_stralgo` to return `list` instead of `tuple` for positions |

**Implementation notes:**
- XREAD/XREADGROUP: this is a significant structural change for RESP2 users
- GEOPOS: both already return `float` values — just switch `tuple` → `list`

**Unit test fixes (`tests/test_commands.py`):**
- `test_xread` (lines ~6088-6114): replace `assert_resp_response(r, r.xread(...), [[stream, [...]]], {stream: [...]})` → `assert r.xread(...) == {stream: [...]}` (4 assertions); empty case: `assert r.xread(...) == {}`
- `test_xreadgroup` (lines ~6132-6160): same pattern as xread (3+ assertions)
- `test_xreadgroup` autoclaim section (line ~6283): replace `assert_resp_response` with dict assertion
- `test_geopos` (line ~4942, ~4959): replace `assert_resp_response(r, ..., [tuple(...)], [list(...)])` → `assert ... == [list(...)]`
- `test_stralgo` (lines ~3027-3040): replace 3 `assert_resp_response` calls with single dict assertions using `list` for positions

---

## Batch 7: Sentinel Commands — flags `str` → `set`, masters structure

**Theme:** Sentinel commands have different flags representation and masters structure. Unify to RESP2 format for SENTINEL MASTERS (dict keyed by name), and unify flags to `set` across all sentinel commands.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | SENTINEL MASTER | `dict` (flags=comma-separated `str`) | `dict` (flags=`set`) | `dict` (flags=`set`, typed values) | Update RESP2 `parse_sentinel_state` to produce flags as `set` instead of comma-separated `str` |
| 2 | SENTINEL MASTERS | `dict[name→state_dict]` | `list[dict]` | `dict[name→state_dict]` (flags=`set`) | Keep RESP2 format (dict keyed by name). Update RESP3 `parse_sentinel_masters_resp3` to build `dict[name→state_dict]` instead of `list[dict]` |
| 3 | SENTINEL SENTINELS | `list[dict]` (flags=`str`) | `list[dict]` (flags=`set`) | `list[dict]` (flags=`set`) | Update RESP2 to split flags into `set` (via shared `parse_sentinel_state`) |
| 4 | SENTINEL SLAVES | `list[dict]` (flags=`str`) | `list[dict]` (flags=`set`) | `list[dict]` (flags=`set`) | Same as SENTINELS |

**Implementation notes:**
- SENTINEL MASTERS keeps RESP2's `dict[name→state_dict]` structure because the Sentinel client (`discover_master`) relies on `masters.get(service_name)` for O(1) lookup. Changing to `list[dict]` would break this internal usage and degrade ergonomics for users.
- RESP3's `parse_sentinel_masters_resp3` must be updated to build `{state["name"]: state for state in masters}` to match.
- The flags change (`str` → `set`) is safe for internal usage because `check_master_state` and `filter_slaves` only read derived boolean fields (`is_master`, `is_sdown`, etc.), never the raw `flags` field.

**Unit test fixes:**
- `tests/test_sentinel.py` / `tests/test_commands.py`: update any sentinel tests that check `flags` as comma-separated string → check as `set`
- SENTINEL MASTERS: no structural change needed in tests (stays dict keyed by name)
- Any `assert "master" in result["flags"]` (string substring) → `assert "master" in result["flags"]` (set membership — same syntax but different semantics, verify correctness)

---

## Batch 8: COMMAND, ACL LOG, ACL GETUSER, XINFO, MEMORY STATS, DEBUG OBJECT

**Theme:** Various commands with heterogeneous differences. Unify to RESP3-closest format.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | COMMAND | `dict` (flags=`list[str]`) | `dict` (flags=`set[str]`, +`acl_categories`) | `dict` (flags=`set[str]`, +`acl_categories` when available) | Change RESP2 `parse_command` to use `set` for flags; add `acl_categories` if present |
| 2 | ACL LOG | `list[dict]` (age-seconds=`float`, client-info=`dict`) | `list[dict]` (age-seconds=`str`, client-info=`str`) | `list[dict]` (age-seconds=`float`, client-info=parsed `dict`) | Add RESP3 callback that applies float/dict parsing (match RESP2 semantic richness) |
| 3 | ACL GETUSER | `dict` (selectors=`list[list[str]]`) | `dict` (selectors=`list[dict[str,str]]`) | `dict` (selectors=`list[dict[str,str]]`) | Change RESP2 path in `parse_acl_getuser` to convert flat selector lists → dicts |
| 4 | XINFO CONSUMERS | `list[dict]` (all values `bytes`) | `list[dict]` (numeric values `int`) | `list[dict]` (str keys, native-typed values) | Change RESP2 `parse_list_of_dicts` to preserve/cast numeric types |
| 5 | XINFO GROUPS | `list[dict]` (all values `bytes`) | `list[dict]` (numeric values `int`) | `list[dict]` (str keys, native-typed values) | Same as XINFO CONSUMERS |
| 6 | MEMORY STATS | `dict` (str keys, str values) | `dict` (str keys, int/bytes values) | `dict` (str keys, native-typed values: `int`/`bytes`) | Change RESP2 to preserve native int values, don't stringify everything |
| 7 | DEBUG OBJECT | Parsed `dict` | Raw `str`/`bytes` | Parsed `dict` | Add RESP3 callback that applies `parse_debug_object` |

**Implementation notes:**
- ACL LOG: exception to RESP3-preference — RESP2's parsed format is semantically richer, so unify **towards RESP2** (float for age-seconds, dict for client-info)
- DEBUG OBJECT: exception — parsed dict is more useful than raw string, so apply RESP2's parsing to RESP3 too
- XINFO: RESP2 currently returns all values as `bytes` strings via `pairs_to_dict`, need to add type casting

**Unit test fixes (`tests/test_commands.py`):**
- `test_command` (line ~7086): if testing flags type, update assertions from `list` to `set`; verify `acl_categories` key is present
- `test_acl_log` (lines ~420-463): `assert_resp_response_in(r, "client-info", expected, expected.keys())` → after unification, both protocols return `dict` for `client-info` so simplify to `assert "client-info" in expected`; verify `age-seconds` is `float` in both
- `test_acl_getuser_setuser` (line ~264): update selector assertions from `list[list[str]]` → `list[dict[str,str]]` format
- `test_xinfo_consumers` (line ~5821): update assertions to expect native-typed values (`int` for numeric fields) instead of `bytes` strings
- `test_xinfo_stream` (line ~5848), `test_xinfo_stream_full` (line ~5868), `test_xinfo_stream_idempotent_fields` (line ~5893): same type normalization
- `test_memory_stats` (line ~6990): update assertions to expect `int` values instead of stringified values
- `test_debug_object`: if tested, verify both protocols return parsed `dict`

---

## Batch 9: `bool` vs raw string — BGREWRITEAOF, BGSAVE

**Theme:** RESP2 returns `True`, RESP3 returns raw status string. Unify to `bool`.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | BGREWRITEAOF | `True` (bool) | `"Background append only file rewriting started"` | `bool` (`True`) | Add RESP3 callback: `lambda r: True` |
| 2 | BGSAVE | `True` (bool) | `"Background saving started"` | `bool` (`True`) | Add RESP3 callback: `lambda r: True` |

**Implementation notes:**
- Exception to RESP3-preference: `bool` is the semantic type — the status string has no useful information beyond "it worked"

**Unit test fixes (`tests/test_commands.py`):**
- `test_bgsave` (line ~1541): currently `assert r.bgsave()` — this already passes since `True` is truthy; no change needed unless there's an `assert_resp_response` elsewhere
- If any test uses `assert_resp_response(r, r.bgsave(), True, "Background saving started")` → replace with `assert r.bgsave() is True`
- Same for BGREWRITEAOF

---

## Batch 10: Module Commands — Probabilistic (BF, CF, CMS, TopK, TDigest)

**Theme:** Two sub-themes: (a) RESP2 parses into custom Info objects; RESP3 returns raw dicts — unify to Info objects. (b) TOPK commands use `parse_to_list` which incorrectly coerces user-provided item names to int/float — fix by using command-specific parsers that respect the semantic type of each response.

**Related issue:** [#3573](https://github.com/redis/redis-py/issues/3573) — `topk().list` decodes "infinity" as float. Fixed partially in PR #3586 (added special-value handling for "infinity"/"nan"/"-infinity") but the broader problem remains: any numeric-looking user string (e.g., `"42"`) is still coerced to `int(42)` by `parse_to_list`. This batch fixes that.

### Sub-batch 10a: INFO commands — unify to Info objects

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | BF.INFO | `BFInfo` object | Raw `dict` | `BFInfo` object | Add RESP3 module callback that constructs `BFInfo` from dict |
| 2 | CF.INFO | `CFInfo` object | Raw `dict` | `CFInfo` object | Add RESP3 module callback that constructs `CFInfo` from dict |
| 3 | CMS.INFO | `CMSInfo` object | Raw `dict` | `CMSInfo` object | Add RESP3 module callback |
| 4 | TOPK.INFO | `TopKInfo` object | Raw `dict` | `TopKInfo` object | Add RESP3 module callback |
| 5 | TDIGEST.INFO | `TDigestInfo` object | Raw `dict` | `TDigestInfo` object | Add RESP3 module callback |

**Implementation notes (10a):**
- Info classes (`BFInfo`, `CFInfo`, etc.) currently expect a flat list `[key, val, key, val]` — RESP3 sends a dict. Need to update constructors or add factory methods that accept both formats
- Exception to RESP3-preference: Info objects provide better API than raw dicts

### Sub-batch 10b: TOPK item-returning commands — remove incorrect callbacks

TOPK.ADD, TOPK.INCRBY, and TOPK.LIST return **user-provided item names** (opaque strings). The current RESP2 parser `parse_to_list` aggressively coerces every element via `int()` then `float()`, which silently converts user strings like `"42"` → `42` and `"3.14"` → `3.14`. This is semantically incorrect — these are key names, not numeric values.

The fix is to **remove the RESP2 callbacks entirely** — no new callback is needed for either protocol. Both RESP2 and RESP3 parsers already return native Python lists, and the connection layer already handles `bytes→str` decoding based on the `decode_responses` setting. A callback would be a no-op at best, and lossy at worst.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 6 | TOPK.ADD | `list` (via `parse_to_list` — coerces to int/float) | `list` (no callback — correct) | `list[str\|None]` or `list[bytes\|None]` | **Remove** RESP2 callback. No RESP3 callback needed. |
| 7 | TOPK.INCRBY | `list` (via `parse_to_list` — coerces to int/float) | `list` (no callback — correct) | `list[str\|None]` or `list[bytes\|None]` | Same |
| 8 | TOPK.LIST | `list` (via `parse_to_list` — coerces to int/float) | `list` (no callback — correct) | `list[str]` or `list[bytes]` | Same |

**Design rationale — no callback needed:**
Both RESP2 and RESP3 protocol parsers return array responses as Python `list`. The connection layer handles encoding/decoding based on `decode_responses`:
- `decode_responses=True` → items arrive as `str` — correct as-is
- `decode_responses=False` → items arrive as `bytes` — correct as-is

The existing `parse_to_list` callback was harmful because it called `nativestr()` on every element, which:
1. **Violates `decode_responses=False`**: Users who explicitly opt out of decoding still get `str` instead of `bytes`
2. **Lossy for non-UTF-8 binary data**: Non-UTF-8 bytes become `�` (U+FFFD replacement character) — data is permanently lost
3. **Converts `"null"` to `None`**: `nativestr()` maps the string `"null"` to Python `None`, silently dropping user data
4. **Numeric coercion**: `int()`/`float()` coercion changes the type of user-provided opaque strings

By removing the callback entirely, the response passes through untouched from the connection layer — which is already doing the right thing.

**Behavioral changes (RESP2):** This is a **correctness fix** that changes existing RESP2 behavior:
- Before: `topk().add("k", "42")` → displaced item `"42"` returns as `42` (int)
- After: `topk().add("k", "42")` → displaced item `"42"` returns as `"42"` (str) or `b"42"` (bytes if `decode_responses=False`)
- Before: `topk().add("k", "null")` → displaced item `"null"` returns as `None`
- After: `topk().add("k", "null")` → displaced item `"null"` returns as `"null"` (str)
- Before: `decode_responses=False` + non-UTF-8 binary → lossy `str` with `�` characters
- After: `decode_responses=False` + non-UTF-8 binary → raw `bytes` preserved faithfully
- This aligns with the fix direction established in issue #3573 / PR #3586

### Sub-batch 10c: TDIGEST numeric commands — keep numeric coercion (correct here)

These commands return **actual numeric values** (quantile boundaries, probabilities) — `parse_to_list` with int/float coercion is semantically correct.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 9 | TDIGEST.BYRANK | `list[float\|str]` (via `parse_to_list`) | Raw `list` | `list[float\|str]` | Add RESP3 callback: `parse_to_list` |
| 10 | TDIGEST.BYREVRANK | `list[float\|str]` | Raw `list` | `list[float\|str]` | Add RESP3 callback: `parse_to_list` |
| 11 | TDIGEST.CDF | `list[float]` | Raw `list` | `list[float]` | Add RESP3 callback: `parse_to_list` |
| 12 | TDIGEST.QUANTILE | `list[float\|str]` | Raw `list` | `list[float\|str]` | Add RESP3 callback: `parse_to_list` |

**Note:** `parse_to_list` keeps "infinity"/"nan"/"-infinity" as strings (per PR #3586 fix). For TDIGEST, these represent valid mathematical boundaries (e.g., `TDIGEST.BYRANK` returns `"inf"` for out-of-range ranks). Keeping them as strings is acceptable; converting to `float('inf')` would also be defensible but would re-introduce the issue from #3573 for hypothetical edge cases.

### Implementation order

1. Remove `parse_to_list` from `_TOPKBloomBase._RESP2_MODULE_CALLBACKS` for ADD/INCRBY/LIST (no replacement needed)
2. Add `_RESP3_MODULE_CALLBACKS` to `_TOPKBloomBase` with `TopKInfo` for INFO only
3. Add `_RESP3_MODULE_CALLBACKS` to `_TDigestBloomBase` with `parse_to_list` for numeric commands + `TDigestInfo` for INFO
4. Add `_RESP3_MODULE_CALLBACKS` to BF/CF/CMS base classes for INFO commands
5. Update Info class constructors to accept dict input (RESP3 format)
6. Update tests in `tests/test_bloom.py` and `tests/test_asyncio/test_bloom.py`

**Unit test fixes (`tests/test_bloom.py`, `tests/test_asyncio/test_bloom.py`):**
- BF.INFO tests: replace `assert_resp_response` with single `assert info.capacity == N` style assertions
- CF.INFO tests: same pattern — unify to `CFInfo` attribute access
- CMS.INFO tests: same pattern — unify to `CMSInfo`
- TOPK.INFO tests: unify to `TopKInfo` attribute access
- TOPK.ADD/LIST/INCRBY tests: replace `assert_resp_response` with single assertion; verify items are always `str` (not `int`/`float`)
- TDIGEST.INFO tests: unify to `TDigestInfo` attribute access
- TDIGEST.BYRANK/CDF/QUANTILE tests: replace dual assertions with single unified `list` assertion

---

## Batch 11: Module Commands — TimeSeries (TS)

**Theme:** RESP2 parses TS responses into structured objects; RESP3 returns raw. Unify to parsed objects.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | TS.GET | `tuple(int, float)\|None` | Raw response | `list[int, float]\|None` | Update RESP2 `parse_get` to return `list`; add RESP3 callback |
| 2 | TS.INFO | `TSInfo` object | Raw response | `TSInfo` object | Add RESP3 callback constructing `TSInfo` from dict |
| 3 | TS.RANGE | `list[tuple(timestamp, float)]` | Raw response | `list[list[timestamp, float]]` | Update RESP2 `parse_range` to return `list`; add RESP3 callback |
| 4 | TS.REVRANGE | Same as TS.RANGE | Raw response | `list[list[timestamp, float]]` | Same |
| 5 | TS.MGET | Parsed dict structure | Raw response | Parsed dict structure | Add RESP3 callback with same parsing |
| 6 | TS.MRANGE | Parsed dict structure | Raw response | Parsed dict structure | Add RESP3 callback |
| 7 | TS.MREVRANGE | Parsed dict structure | Raw response | Parsed dict structure | Same |
| 8 | TS.QUERYINDEX | `list` | Raw response | `list` | Add RESP3 callback: `parse_to_list` |

**Implementation notes:**
- `parse_range` returns `list[tuple]` → change to `list[list]` (Batch 1 principle: list over tuple)
- `parse_get` returns `tuple(int, float)` → change to `list[int, float]`
- TSInfo constructor needs to handle dict input (RESP3) in addition to flat list (RESP2)

**Unit test fixes (`tests/test_timeseries.py`):**
- `test_get` (lines ~200-204): replace `assert_resp_response(client, client.ts().get(2), (5, 1.5), [5, 1.5])` → `assert client.ts().get(2) == [5, 1.5]` (3 assertions)
- `test_range` / `test_range_advanced` (lines ~253, ~291, ~295, ~297): replace `assert_resp_response(client, res, [(0, 10.0), (10, 1.0)], [[0, 10.0], [10, 1.0]])` → `assert res == [[0, 10.0], [10, 1.0]]`
- `test_range_latest` (lines ~312-321): same tuple→list unification
- `test_range_empty` (lines ~370-402): same pattern
- `test_revrange_latest` (lines ~429-470): same pattern
- `test_mget` / `test_mget_latest` (lines ~972-999): replace `assert_resp_response` with unified dict structure assertions
- `test_mrange` / `test_mrevrange` (lines ~517-549, ~883-931): same pattern
- `test_info` (lines ~39-139): replace `assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))` → use unified `TSInfo` attribute access
- `test_queryindex` (line ~1028): replace `assert_resp_response(client, ..., [2], ["2"])` → unified `list` assertion
- **~70+ `assert_resp_response` calls** across the file need updating — this is a high-volume batch

Also update `tests/test_asyncio/test_timeseries.py` with identical changes.

---

## Batch 12: Module Commands — JSON

**Theme:** RESP2 applies `self._decode`; RESP3 returns raw. Unify to decoded format.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | JSON.ARRAPPEND | Decoded | Raw response | Decoded | Add RESP3 callback applying `_decode` |
| 2 | JSON.ARRINDEX | Decoded | Raw response | Decoded | Same |
| 3 | JSON.ARRINSERT | Decoded | Raw response | Decoded | Same |
| 4 | JSON.ARRLEN | Decoded | Raw response | Decoded | Same |
| 5 | JSON.ARRTRIM | Decoded | Raw response | Decoded | Same |
| 6 | JSON.NUMINCRBY | Decoded | Raw response | Decoded | Same |
| 7 | JSON.NUMMULTBY | Decoded | Raw response | Decoded | Same |
| 8 | JSON.OBJKEYS | Decoded | Raw response | Decoded | Same |
| 9 | JSON.OBJLEN | Decoded | Raw response | Decoded | Same |
| 10 | JSON.STRAPPEND | Decoded | Raw response | Decoded | Same |
| 11 | JSON.STRLEN | Decoded | Raw response | Decoded | Same |

**Implementation notes:**
- JSON module has instance-level decoder — RESP3 callbacks need access to `self._decode`
- May need to register RESP3 callbacks at instance level (in `__init__`) similar to how RESP2 callbacks are registered

**Unit test fixes (`tests/test_json.py`):**
- `test_json_setgetjson` and related (lines ~133-164): replace `assert_resp_response(client, client.json().get("arr"), [], [])` → `assert client.json().get("arr") == []` (already same in this case)
- `assert_resp_response(client, client.json().type("1"), "integer", ["integer"])` → unify to single expected format (likely `["integer"]` or `"integer"` — decide based on final decode behavior)
- Multiple `assert_resp_response` calls throughout the file: after unification both protocols return decoded format → replace with single `assert` statements
- Also update `tests/test_asyncio/test_json.py` with identical changes

---

## Batch 13: Module Commands — Search (FT)

**Theme:** RESP2 applies custom parsing; RESP3 returns raw. Unify to parsed format.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | FT.INFO | Parsed dict | Raw response | Parsed dict | Add RESP3 `_parse_info` handling dict input |
| 2 | FT.SEARCH | `Result` object | Raw response | `Result` object | Add RESP3 `_parse_search` handling dict/map input |
| 3 | FT._HYBRID | Parsed | Raw response | Parsed | Add RESP3 callback |
| 4 | FT.AGGREGATE | Parsed | Raw response | Parsed | Add RESP3 callback |
| 5 | FT.PROFILE | Parsed | Raw response | Parsed | Add RESP3 callback |
| 6 | FT.SPELLCHECK | Parsed | Raw response | Parsed | Add RESP3 callback |
| 7 | FT.CONFIG GET | Parsed | Raw response | Parsed | Add RESP3 callback |
| 8 | FT.SYNDUMP | Parsed | Raw response | Parsed | Add RESP3 callback |

**Implementation notes:**
- Search module is complex — parsers assume RESP2 flat list format
- Need to create RESP3-aware versions of each parser or make existing parsers handle both formats
- `Result` object construction needs to work from RESP3 dict format
- ⚠️ This is the highest-effort batch

**Unit test fixes (`tests/test_search.py`):**
- Search tests are extensive — many test `Result` objects, `AggregateResult`, spell check results
- After unification, both protocols return parsed `Result` objects → any `assert_resp_response` or `is_resp2_connection` branching in search tests should be replaced with single assertions
- Check for tests that directly inspect raw response structure vs `Result` attributes
- Also update `tests/test_asyncio/test_search.py` with identical changes
- ⚠️ Search tests are the most complex to update — may need to run tests iteratively to catch all differences

---

## Batch 14: Module Commands — VectorSet

**Theme:** RESP2 parses; RESP3 returns raw. Unify.

| # | Command | Current RESP2 Type | Current RESP3 Type | Final Type | Action |
|---|---------|-------------------|-------------------|------------|--------|
| 1 | VINFO | `dict` (via `pairs_to_dict`) | Raw `dict` (native map) | `dict` | Already equivalent — just verify key types match; may need RESP3 callback for str key normalization |
| 2 | VLINKS | Parsed `list[list]\|list[dict]` | Raw response | Parsed `list[list]\|list[dict]` | Add RESP3 callback applying `parse_vlinks_result` |

**Unit test fixes (`tests/test_vectorset.py`):**
- Check for any `assert_resp_response` calls related to VINFO/VLINKS — if present, replace with single assertions
- VINFO: verify key type consistency (str vs bytes keys in dict) — may need minor assertion updates
- VLINKS: verify parsed structure is identical after adding RESP3 callback

---

## Implementation Priority & Risk Assessment

| Batch | Commands | Risk | Effort | Priority |
|-------|----------|------|--------|----------|
| 1 | 9 | Medium — tuple→list breaks `isinstance(x, tuple)` | Low — callback swap | 🔴 High |
| 2 | 2 | Medium — flat→nested breaks `len()` usage | Medium — new callbacks | 🔴 High |
| 3 | 4 | Medium — tuple→list same as Batch 1 | Low | 🔴 High |
| 4 | 3 | Medium — structure changes | Medium — new callbacks | 🟡 Medium |
| 5 | 11 | 🔴 High — `str`→`bytes` breaks string comparisons | Low — remove callbacks | 🟡 Medium |
| 6 | 5 | 🔴 High — XREAD dict structure completely different | Medium | 🟡 Medium |
| 7 | 4 | 🔴 High — SENTINEL MASTERS structure change | Medium | 🟡 Medium |
| 8 | 7 | Medium — mixed changes | High | 🟡 Medium |
| 9 | 2 | Low — adding missing RESP3 callback | Low | 🟢 Low |
| 10 | 12 | Low — adding missing RESP3 callbacks | Medium — Info class updates | 🟡 Medium |
| 11 | 8 | Low — adding missing RESP3 callbacks | Medium — parser updates | 🟡 Medium |
| 12 | 11 | Low — adding missing RESP3 callbacks | Medium — instance callbacks | 🟡 Medium |
| 13 | 8 | Medium — complex parsers | 🔴 High — Search is complex | 🔴 High |
| 14 | 2 | Low | Low | 🟢 Low |

**Total: ~88 commands across 14 batches**

### Suggested Execution Order

1. **Batch 9** (BGREWRITEAOF, BGSAVE) — simplest, low risk, builds confidence
2. **Batch 1** (Sorted set score pairs) — high impact, moderate effort
3. **Batch 3** (Blocking pops) — same principle as Batch 1
4. **Batch 2** (ZRANDMEMBER, HRANDFIELD) — related to Batch 1
5. **Batch 4** (ZMPOP, BZMPOP, LCS) — related raw protocol diffs
6. **Batch 5** (str vs bytes) — high impact, but simple code changes
7. **Batch 6** (XREAD, GEOPOS, etc.) — structural changes
8. **Batch 7** (Sentinel) — structural changes
9. **Batch 8** (COMMAND, ACL LOG, etc.) — heterogeneous
10. **Batch 14** (VectorSet) — quick win
11. **Batch 10** (Probabilistic modules) — module callbacks
12. **Batch 11** (TimeSeries) — module callbacks
13. **Batch 12** (JSON) — module callbacks
14. **Batch 13** (Search) — most complex, save for last

