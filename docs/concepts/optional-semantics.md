# Optional semantics

One `optional(...)` is a single all-or-nothing block. The parameters inside it are kept together or the whole block is removed. A mix of present and missing values is a compile error.

Grammar and keyword forms: [Optional DSL](../reference/optional-dsl.md). How absence is detected: [Compile and elision](compile-and-elision.md).

## All or nothing

```sql
--($args.active integer, $args.ids integer[], $args.name string)--
where optional(
    u.active = {{$args.active}}
    and u.user_id in ({{$args.ids}})
    and u.user_name = {{$args.name}}
)
```

Fixture: `tests/fixtures/api/user/optional_multi/`.

| Args | What happens |
|---|---|
| all three given | block kept; every value bound |
| all omitted, null, or `[]` | block removed, plus the preceding `and` / `or` |
| only some given | compile error: `partial parameters: …` |

The same name used twice counts once. The block is kept only when **every** listed parameter is provided (non-empty for arrays).

## What “provided” means

| Value | Effect |
|---|---|
| omitted or `null` | absent |
| array or blob `[]`, blob `""` or `"[]"` | absent |
| scalar `0`, `""`, or `false` | present |
| header default, caller omitted the arg | present, so the filter stays |

A `bool` used as an `optional_when` **condition** follows the same absence rule. `false` is present, so the block stays. Pass `null` or omit the arg to drop it.

## `optional_when`

`optional(...)` decides from parameters **inside** the block. `optional_when({{cond}}, body)` adds a condition that is not emitted and never bound.

An absent condition drops the block first, which also skips the body’s partial-parameter check. If the condition is present and the body is only partly filled, compile still errors.

The body still needs at least one `{{param}}` of its own, or an `optional_groups_*`. Nesting `optional_when` directly inside `optional_when` is rejected.

## Nesting matrix

| Pattern | Supported? |
|---|---|
| `optional_groups_*` inside `optional(...)` | Yes. The blob does not join the optional’s all-or-nothing set. The group elides itself when it has no rows. |
| `optional(...)` wrapping only a group | Yes. With no params of its own, the wrapper disappears with the group instead of compiling to `()`. |
| `optional(...)` inside a group body | Yes if at least one header-declared `{{$args.*}}` gates it. A block keyed only on row fields is a compile error, because those fields are always present. |
| `optional_groups_*` inside `optional_groups_*` | No. `nested optional_groups_or(...)/optional_groups_and(...) is not supported`. |
| `optional_when` inside `optional_when` | No. `nested optional_when(...) is not supported`. |
| `optional(optional(...))` | Parses, but the gates are **not** independent. The outer block collects every `{{param}}` in its body, including parameters that belong to the inner `optional`. Omit only the inner parameter and compile fails with a partial-parameter error instead of dropping just the inner block. |

Inside a group body, an `optional_when` condition must be a header-declared `$args.*` name. A bare name would be a row field, which cannot gate anything. With a condition in place, the body may use only row fields.

## Worked nesting

```sql
and optional(
  u.active = {{$args.active}}
  and optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}}))
)
```

| Args | Result |
|---|---|
| `pairs` and `active` both given | both the scalar predicate and the group |
| only `active` | scalar predicate; the group drops out |
| `active` omitted | the whole `optional(...)` is removed |

```sql
optional_groups_or(
  {{$args.pairs}},
  col1 = {{cv1}} and optional(col2 = {{$args.flag}})
)
```

`flag` omitted: the inner optional drops in every branch. `optional(col1 = {{cv1}})` alone is an error, because `cv1` is a row field.

Fixture: `user/groups_in_optional`. Conditional form: `user/when_optional`.
