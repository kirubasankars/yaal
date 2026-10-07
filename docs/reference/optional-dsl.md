<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Optional DSL

Behavior (all-or-nothing, nesting, what is absent): [Optional semantics](../concepts/optional-semantics.md).

## `optional(body)`

```sql
and optional(u.user_id = {{$args.id}})
```

| Call | Result |
|---|---|
| value present | `and (u.user_id = ?)` with a bind |
| value null or omitted | clause removed |

The body must contain at least one `{{param}}`, or an `optional_groups_*`. Multiple parameters, including arrays, are one set: all provided, or all omitted. A partial set raises:

```text
optional(...) requires every parameter to be provided or all omitted; partial parameters: ...
```

If the elided filter was the only predicate, empty `WHERE`, `PREWHERE`, or `HAVING` is dropped, including before `)` or the next clause. Author-written `WHERE 1 = 1 AND …` is cleaned the same way. Each filter clause is cleaned independently.

Long form, parenthesized, case-insensitive: `({{param}} is null or col = {{param}})`.

## `optional_when({{cond}}, body)`

```sql
--($args.apply bool, $args.id integer)--
and optional_when({{$args.apply}}, u.user_id = {{$args.id}})
```

| `$args.apply` | `$args.id` | Result |
|---|---|---|
| omitted or null | given | clause removed |
| `true` or `false` | given | `and (u.user_id = ?)` |
| given | omitted | clause removed |

The condition is not emitted and is not bound. Only absence disables the block. An absent condition is checked first, so it also suppresses the body’s partial-parameter error.

The condition must be declared in the header. The body needs its own `{{param}}` or an `optional_groups_*`. Nesting `optional_when` inside `optional_when` is a compile error.

Inside a group body the condition must be `{{$args.*}}`. With a condition, the body may use only row fields:

```sql
optional_groups_or(
  {{$args.pairs}},
  c1 = {{cv}} and optional_when({{$args.flag}}, c2 = {{cv2}})
)
```

Fixture: `user/when_optional`.
