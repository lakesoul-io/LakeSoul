# Text Search Ranking and ES-Compatible Scoring — Design Note

Status: draft, tracked for follow-up pull requests.

## Summary

The text index landed with **exact filtering** semantics: a query returns the
rows that really match, but BM25 scores are discarded at the reader boundary and
there is no global relevance ordering. That is sufficient for `WHERE
text_match(...) LIMIT k`-style filtering and for per-shard candidate scans.

An **Elasticsearch-compatible API gateway** over LakeSoul (for RAG/knowledge-base
clients that speak the ES v7/v8 driver contract) changes the requirement: its
`_search` responses must carry a non-null `_score` per hit and return hits in
descending relevance order, because downstream hybrid retrieval fuses vector and
keyword results by **rank** (weighted RRF), not by raw score. This note records
the scoring contract and the design that satisfies it.

## Current implementation

- `rust/lakesoul-text/src/search.rs`: BM25 top-k **per split**, merged by
  primary key (`merge_hits` keeps the best score and sorts descending).
- `rust/lakesoul-io/src/reader.rs`: the merged hits are converted to primary
  keys and injected as a `pk IN (...)` filter; scores are dropped
  (`rust/lakesoul-io/src/index/candidate.rs`).
- `rust/lakesoul-text/src/verify.rs`: `matching_scores` already computes the
  **current-row** BM25 scores of the candidate batch, because the exact
  verification pass builds a small in-memory index over the merged rows.
- `rust/lakesoul-datafusion`: `text_match(column, query)` is a boolean UDF; the
  pushdown rule only matches queries with a finite `LIMIT`.
- Python/Daft: `text_search` returns the exact matches among the per-shard
  candidates; the total is bounded by `top_k × number_of_shards`.

So scores exist twice (index search, exact verification) and are thrown away
once (candidate injection).

## Options

| Option | Description | Correctness | Cost |
|--------|-------------|-------------|------|
| (a) Keep exact filtering only | Document `top_k` semantics; no scores or ordering | Exact matches, arbitrary order | None; cannot serve ES-compatible clients |
| (b) Scored candidates + approximate global merge | Per-shard top-k carry scores; a coordinator (DataFusion Sort / Daft sort) merges by score and truncates to a global `size` | Ordering is approximate across shards (per-shard BM25 statistics), equivalent to ES's default non-DFS behavior | Medium: score propagation, over-fetch for verification, planner/Daft changes |
| (c) True global BM25 | Global collection statistics or a two-phase fetch protocol | Exact global ranking | Large: conflicts with immutable per-split statistics; needs a statistics exchange |

**Recommendation: (b).** It makes keyword results globally ordered (which is all
the ES contract and rank-based fusion need), reuses the existing candidate
scan, and matches ES's own default semantics. Option (c) can be evaluated later
as a `dfs_query_then_fetch`-style enhancement.

## ES-compatible scoring contract

Observed from the ES v7/v8 driver behavior of an ES-compatible RAG client, the
retrieval contract that matters for BM25:

1. **`_score` must be present and numeric on every keyword hit.** The v8 typed
   client dereferences `*hit.Score_` without a nil check (a null score panics);
   the v7 client drops hits whose `_score` is not a JSON number.
2. **Hits must arrive in descending BM25 order.** The client never sends a
   `sort` clause and derives the fusion rank from the array position
   (1-indexed).
3. **Filters must not participate in scoring.** Base conditions
   (`knowledge_base_id`/`knowledge_id`/`tag_id` terms, `is_enabled` exclusion)
   are placed in `bool.filter`; only `match` is in `bool.must`. A row with a
   missing `is_enabled` field counts as enabled.
4. **No keyword `min_score`/threshold.** The ES drivers push thresholds only on
   the vector path. Applying a keyword threshold with a different BM25 scale
   would silently reduce recall.
5. **No normalization and no cross-modality comparability.** Hybrid retrieval
   is weighted RRF on ranks (`w_v/(k+rank_v) + w_k/(k+rank_k)`); raw BM25
   values are discarded. Keyword-only results are rescaled by their maximum
   (only when it exceeds 1), which preserves order.
6. **Scores must be finite, monotonic with relevance, and not all equal** so
   that the rank order is meaningful; clients clamp NaN/Inf defensively.
7. **The analyzer is defined by the compatibility layer.** Chinese segmentation
   is our `jieba` configuration; the client does not pass analyzer settings.

Additional non-scoring API facts that shape the gateway (not the ranking
design): the driver uses nine endpoints (`GET /`, `HEAD`/`PUT /{index}`,
`GET /{index}/_mapping`, `POST /{index}/_doc|_bulk|_delete_by_query|_search`,
`POST /{index}/_update_by_query`); the query DSL subset is
`bool(filter/must/must_not)`, `terms`, `term`, `match`, `script_score`,
`min_score`, `from/size`, `_source.excludes`; keyword `match` appears in both
the v7 shorthand string form and the v8 object form; v7 sends no `size`
(ES default 10) while v8 sends `size = TopK`; responses need the
`X-Elastic-Product: Elasticsearch` header, version 8.x, an index mapping with
`chunk_id` as `keyword` (or `.keyword` subfields), and near-real-time
write-to-search visibility.

Also required: the keyword `_search` must return the **globally** top `size`
scored hits of the requested `bool` query, including `from/size` pagination for
the filter-only copy path (which does not consume scores).

## Design for (b)

### Scored candidate path

- The reader gains an option (`text_search_scores=true`) that adds a hidden
  `__lakesoul_text_score: Float32` column to the scan output.
- The score is taken from the exact verification pass
  (`verify::matching_scores`), so it reflects the **current** row's BM25 score
  and rows dropped as stale never carry a score. This keeps one source of truth
  for both "does it match" and "how well".
- The column is internal: it is only present when requested, and SQL/Daft drop
  it unless the caller asks for it (same pattern as the hidden text column used
  by verification).

### Global merge

- Each shard returns its top-`candidate_k` scored hits. `candidate_k` is
  `max(size × over_fetch, size + stale_budget)` with an `over_fetch` factor
  (default 2–3) so the exact verification pass can drop stale candidates
  without leaving the final result short of `size`.
- A coordinator merges the per-shard results by descending score (ties broken by
  primary key for determinism) and truncates to `size`:
  - DataFusion: the existing outer `Sort`/`Limit` above the scan is enough once
    the score column is visible; the pushdown rule additionally recognizes
    `ORDER BY text_score(...) LIMIT k` and rewrites the score expression to the
    hidden column.
  - Daft: `text_search` sorts the candidates by the hidden column, limits to the
    global `top_k`, and drops the score unless requested.
- Cross-shard comparability is accepted as ES's default does: BM25 statistics
  are per split, so scores from different shards are not perfectly calibrated.
  A future consistent mode (global/DFS-like statistics) can be added behind an
  option without changing the wire contract.

### Verification/size interaction

Because verification runs after the merge inputs are collected, the pipeline is:

```text
per-shard: index top-(size × over_fetch) with scores
    → coordinator merge (candidate pool)
    → exact verification over current rows (scores recomputed)
    → drop stale candidates
    → sort by score, truncate to size
```

If a shard's candidate pool empties below `size` the coordinator may re-query
with a larger `candidate_k` (bounded retry); compaction keeps the number of
stale candidates low.

## API sketches

SQL (DataFusion):

```sql
SELECT id, body, text_score(body, 'quick fox') AS score
FROM documents
WHERE text_match(body, 'quick fox')
ORDER BY score DESC
LIMIT 10;
```

Python/Daft:

```python
# Global top-k (new default), optionally with the score column.
df = text_search(table, "quick fox", top_k=10, with_score=True)

# Low-level scan: per-shard candidates plus scores.
table.scan().options(
    reader_options={
        "text_search_query": "quick fox",
        "text_search_top_k": "30",   # per-shard candidates
        "text_search_scores": "true",
    }
)
```

ES gateway keyword search: `bool.filter` becomes scan filter pushdowns, `match`
becomes the text query, `size` becomes the global truncation, and `_score` is
the BM25 score handed back per hit. `script_score`/`min_score` apply to the
vector path only.

## Migration and compatibility

- Python `text_search` changes semantics from a per-shard candidate union to a
  global top-k. The change is additive for small result sets (which the current
  API already documents as bounded) but should be called out in the changelog;
  callers that want raw candidates keep using `reader_options`.
- SQL without `ORDER BY text_score(...)` keeps today's behavior (no ordering
  guarantee). Making `text_match` imply relevance order is left as an open
  question below.
- `text_search_top_k` keeps its meaning (per-shard candidates); the global size
  is the new API parameter backed by the over-fetch factor.
- Nothing changes for filter-only paths (`from/size` copy scans) or for the
  exact filtering guarantees: verification still runs after merge-on-read.

## Open questions

1. Should a `text_match` filter imply BM25 ordering, or require an explicit
   `ORDER BY text_score(...)`? (Explicit keeps SQL semantics predictable; the
   ES gateway does not go through SQL.)
2. Default `over_fetch` factor and whether to retry on under-filled results.
3. Tie-breaking: primary key ascending is deterministic; some clients may
   expect ES's internal doc-order tie-break, which is not observable.
4. Whether the gateway needs a consistent-statistics mode
   (`dfs_query_then_fetch`-like) for multi-shard keyword queries.
5. Score exposure in the SQL result schema: hidden column vs. `text_score()`
   projection; naming of the internal field.

## Staging

1. **Scored candidates and global top-k** (this design): reader score option,
   `text_score` UDF + planner rule, Daft/Python global ranking, over-fetch,
   tests (cross-bucket ordering, stale/over-fetch interaction).
2. **ES-compatible gateway service**: the nine endpoints, the DSL subset,
   response shapes, product header, mapping handling, NRT visibility; keyword
   search on top of stage 1, vector search on the existing ANN index
   (`script_score`/`cosineSimilarity`/`min_score` semantics).
3. **Enhancements**: consistent cross-shard statistics, highlight/snippet
   support via `stored=true`, and query-time analyzer overrides.
