<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Precompile

**Goal:** compile descriptors once, then still elide optionals per request.

Compile does not need a database. Arg values are **not** baked into the artifact.

## Commands

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  query user/get --arg id=1
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  explain user/list
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  explain user/list --arg active=1
```

The two explains differ: the optional `active` predicate is absent in the first and present in the second. Precompile skipped lexing. It did not skip elision.

`--debug` ignores `--precompiled` and re-reads live SQL.

## Files to open

After compile, look at `/tmp/yaal-precompiled/user/get.json`. One JSON file per operation path. Alternate mappers use `user/get#summary.json`.

Details: [Precompile guide](../guides/precompile.md) and [Precompiled artifacts](../reference/precompiled-artifacts.md).

## Your own API

Checklist for a new folder:

1. Create `api/<area>/<op>/`.
2. Add at least one `*.sql`.
3. Add a parameter header and `{{...}}` binds.
4. Add `$.output.json`.
5. Run `yaal query <area>/<op> --api ... --db ...`.

Edit a copy instead of the fixtures: [Experiment sandbox](../guides/experiment-sandbox.md).

Python and C# call the same paths. Snippets: [Python](../appendix/python.md), [C#](../appendix/csharp.md).

## Exercise

Compile to `/tmp/yaal-precompiled`, then explain `user/list` with and without `active`. The SQL must change even though you did not recompile.
