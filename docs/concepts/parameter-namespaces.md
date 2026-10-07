# Parameter namespaces

A `{{name}}` binds from exactly one place. The prefix selects the place. The SQL header declares the type for every name that comes from the caller or from `$params`.

| Prefix | Source | Declared in the header? |
|---|---|---|
| `$args.*` | `query` args (`--arg` / `--args`) | yes |
| bare name | `query` payload (`--payload`) | yes, unless it is a group **row field** |
| `$params.*` | run bag | yes, for names you bind |
| `$parent.*` | parent branch payload | yes |

## `$args` and payload

`$args.id` and a bare `id` are different parameters. They can have different types. Required mark `!` and defaults apply per declaration. The header is the only input model: Yaal builds the args and payload schemas from it (`float` → number, `bool` → boolean).

Invalid types or a missing required value return `{"errors": [...]}` and do not execute.

## `$params`

The run bag starts with values Yaal sets (`$run_id`, and `$last_inserted_id` after a write). `$mode=params` copies every column of that row onto `$params` for later twigs in the same file. Later SQL reads them with `{{$params.total_count}}` and must declare `$params.total_count` in the header.

`$params` does not cross SQL files by itself. Sibling branches are separate files. Pass what they need as `$args`, or read a parent payload with `$parent`.

## `$parent`

A child branch can bind `{{$parent.field}}` from the parent branch payload. Declare it in the child file’s header like any other parameter.

## Group row fields

Inside `optional_groups_or` / `optional_groups_and`, a **bare** placeholder is a key in each blob row, not a payload parameter. It must not be declared as a bare header name. A `$args.` name in that body is still a runtime arg: it must be declared, and it is repeated in every branch.

These are different namespaces, so a row field `cv1` may coexist with a declared `$args.cv1` elsewhere in the statement.

Details: [Optional groups DSL](../reference/optional-groups-dsl.md).
