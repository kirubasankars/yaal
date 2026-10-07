# Optional filters

**Problem:** keep a predicate in the statement, and remove it when the caller does not supply the value.

## SQL

```sql
--($args.active integer)--
select u.user_id, u.user_name, u.active
from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
```

Fixture: `tests/fixtures/api/user/list/$.sql` (also sorts; see [Dynamic ORDER BY](dynamic-order-by.md)).

Several parameters in one block are all-or-nothing. Fixture: `user/optional_multi`.

```sql
where optional(
    u.active = {{$args.active}}
    and u.user_id in ({{$args.ids}})
    and u.user_name = {{$args.name}}
)
```

A condition that is not part of the SQL uses `optional_when`. Fixture: `user/when_optional`.

```sql
--($args.apply bool, $args.id integer)--
and optional_when({{$args.apply}}, u.user_id = {{$args.id}})
```

## Explain

```bash
yaal explain user/list
yaal explain user/list --arg active=1
```

| Call | Expect |
|---|---|
| no `active` | no `active` predicate; binds `[]`; no leftover `where 1 = 1` |
| `active=1` | `(u.active = ?)` and bind `[1]` |

Pass only `active` to `user/optional_multi` and compile fails with `partial parameters`. Pass all three, or none.

`false` on an `optional_when` condition **keeps** the block. Omit `apply` to drop it.

## Rules, briefly

!!! info "Read this before nesting optionals"
    Nested `optional(optional(...))` is not an independent inner filter. The outer block collects the inner parameters too. See [Optional semantics](../concepts/optional-semantics.md).

Long form still works and must be parenthesized: `({{param}} is null or col = {{param}})`.

Grammar: [Optional DSL](../reference/optional-dsl.md).
