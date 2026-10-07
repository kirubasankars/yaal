<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Guides

Task-sized pages. Each one states the problem, shows the SQL, and says what `yaal explain` should show. Behavior is defined in [Concepts](../concepts/index.md). Grammar is in the [Reference](../reference/index.md).

| Task | Guide |
|---|---|
| Drop a filter when an arg is missing | [Optional filters](optional-filters.md) |
| `IN` a list | [Array IN filters](array-in-filters.md) |
| Repeat a predicate per row | [Optional groups](optional-groups.md) |
| Let the client pick sort columns | [Dynamic ORDER BY](dynamic-order-by.md) |
| Map columns and nest joins | [Output shaping](output-shaping.md) |
| Split a nest into another SQL file | [Child SQL and siblings](child-sql-and-siblings.md) |
| Page a list | [Pagination and mode params](pagination-and-mode-params.md) |
| Skip lexing at startup | [Precompile](precompile.md) |
| See SQL without running it | [Explain and debug](explain-and-debug.md) |
| Insert, then select | [Writes and modes](writes-and-modes.md) |
| Edit fixtures safely | [Experiment sandbox](experiment-sandbox.md) |
