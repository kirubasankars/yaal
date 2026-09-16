# StringBuilder vs `optional()` — SQL construction

Additive `StringBuilder` filter appends versus Yaal subtractive `optional()` elision. **SQL construction only** — no database. The question is how much per-request work Yaal pays to subtract unused predicates compared with typical hand-rolled C# that appends `AND` clauses when a value is present.

See [Optional filters](descriptors.md#optional-filters) for the language, and [Why SQL-first fits](why-sql-first.md) for why that subtractive shape exists.

## What was timed

Same reporting-shaped statement at **1, 4, 8, and 16** optional equality filters:

```sql
--($args.f0 integer, $args.f1 integer, ...)--

select id from t
where 1 = 1
  and optional(c0 = {{$args.f0}})
  and optional(c1 = {{$args.f1}})
  -- ...
```

| Method | What the loop measures |
|---|---|
| **StringBuilder** | `select id from t where 1 = 1` then `if (fN != null) sb.Append(" and cN = ?")` plus a bind list. Leaves `where 1 = 1` (usual app pattern; no cleanup). |
| **Yaal Compile** | Registered descriptor, then `SqlCompiler.Compile` — optional-group elision and `WHERE 1 = 1` cleanup only. |
| **Yaal ExplainSql** | Public path after `RegisterDescriptor`: descriptor lookup, `CreateContext`, a new `DataProviderHelper`, then compile. Each call is a **cold compile** (`ExplainSql` does not reuse a compile cache). |

Arg patterns per filter count: **all omitted**, **all present**, **mixed** (every other present).

Before timing, both sides were checked for equivalent SQL (whitespace-normalized, `where 1 = 1` stripped, grouping parens from `optional()` ignored). Yaal keeps `(cN = ?)`; StringBuilder does not. Bind counts matched.

Descriptor parse and `RegisterDescriptor` happen once per N, outside the timed loop.

## Environment

| | |
|---|---|
| Date | 16 Sep 2026 |
| CPU | Intel Core i7-10850H @ 2.70 GHz (12 cores) |
| OS | Fedora Linux 44 (Workstation Edition) |
| Runtime | .NET 10.0.11, **Release** |
| Package | Yaal **0.6.0** |
| Harness | Stopwatch, 80 warmup iterations, `GC.Collect` between cells, ~1 s measure per cell |

Debug builds are not usable for this comparison.

## Headline — 8 mixed filters

| Method | ns/op | vs StringBuilder |
|---|---|---|
| StringBuilder | 173 | 1× |
| Yaal Compile | 2,957 | **17×** |
| Yaal ExplainSql | 7,506 | **43×** |

Yaal is slower on the construction path. Absolute time is still **microseconds**. A local SQLite round-trip is typically tens to hundreds of microseconds; a remote database is milliseconds. This overhead matters in a tight in-memory loop, not under a real query.

## Results

ns/op is the mean over the measured iterations.

| N | Pattern | StringBuilder | Compile | ExplainSql | Compile × | ExplainSql × |
|---|---|---:|---:|---:|---:|---:|
| 1 | omitted | 122.5 | 1,348.1 | 4,224.7 | 11.0 | 34.5 |
| 1 | present | 114.1 | 1,141.6 | 3,549.0 | 10.0 | 31.1 |
| 1 | mixed | 101.2 | 1,134.8 | 3,504.6 | 11.2 | 34.6 |
| 4 | omitted | 83.5 | 1,576.6 | 4,126.1 | 18.9 | 49.4 |
| 4 | present | 167.7 | 2,122.6 | 5,638.4 | 12.7 | 33.6 |
| 4 | mixed | 121.6 | 1,889.9 | 5,088.8 | 15.5 | 41.8 |
| 8 | omitted | 85.0 | 2,191.4 | 5,463.6 | 25.8 | 64.3 |
| 8 | present | 239.3 | 3,505.8 | 8,800.7 | 14.7 | 36.8 |
| 8 | mixed | 173.1 | 2,956.5 | 7,506.3 | 17.1 | 43.4 |
| 16 | omitted | 92.6 | 3,429.6 | 8,332.3 | 37.1 | 90.0 |
| 16 | present | 414.3 | 5,973.3 | 13,732.9 | 14.4 | 33.2 |
| 16 | mixed | 255.5 | 4,908.7 | 11,735.1 | 19.2 | 45.9 |

## How to read it

**Compile is the `optional()` tax.** It walks tokens, drops omitted groups, and cleans an empty or `1 = 1` `WHERE`. StringBuilder never does that work. The gap is about **10–20×** when some filters are present, and grows with N.

**ExplainSql is Compile plus per-request framing** (~2–3× Compile): `CreateContext` (args → `Shape`, a `$run_id` GUID), a new helper, and wrapping the compiled SQL into dictionaries. That is the public API an app actually calls for explain; `Query` uses the same compile helper inside the executor.

**All-omitted is the worst ratio**, not the worst absolute time. StringBuilder only emits the base string (~85 ns). Yaal still visits every optional group to subtract it. At 8 omitted filters that is **26× Compile / 64× ExplainSql**. At 16 omitted, **37× / 90×**.

**All-present is the smallest ratio** (Compile ~14–15× at N≥8) because StringBuilder also does more work — one `Append` and one bind per filter.

**Construction stays cheap in wall time.** The slowest cell in this run is ExplainSql at 16 present filters: **14 µs**.

## Reproduce

The numbers above come from a Release Stopwatch harness that:

1. Writes one temp `$.sql` per N and `RegisterDescriptor`s it.
2. Asserts SQL equivalence, then times the three methods for each (N, pattern) cell.
3. Consumes `sql.Length` and bind count so the JIT cannot drop the work.

`make benchmark-csharp` is a **different** bench (descriptor *load*: live SQL vs JSON vs `RegisterDescriptor`). It does not cover `optional()` vs StringBuilder.
