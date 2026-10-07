# Precompile

**Problem:** avoid re-lexing SQL and JSON on every process start. Elision must still depend on the request.

## JSON artifacts

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  query user/get --arg id=1
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  explain user/list --arg active=1
```

Layout:

```text
/tmp/yaal-precompiled/
  user/get.json
  user/list.json
  report/summary.json
```

Alternate mappers: `user/get#summary.json`.

`--debug` ignores `--precompiled` and reads live files. Elision still runs.

Load order when debug is off: in-memory registration (C#) → memory cache → precompiled JSON → live SQL. See [Precompiled artifacts](../reference/precompiled-artifacts.md).

## C# source

The .NET CLI can emit C# instead of JSON (`--format cs`) and you register the generated branches at startup. That path is the fastest cold start and is covered in the [C# appendix](../appendix/csharp.md). JSON artifacts are the ones Python and C# share.

`make benchmark-csharp` compares load cost (live SQL vs JSON vs generated C#). It does not measure `optional()` versus string appends; that essay is [StringBuilder vs optional](../essays/stringbuilder-vs-optional.md).
