# Nested get

**Goal:** one user object with a nested `roles` array from a join.

## Commands

```bash
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
yaal explain user/get
```

## Files to open

- [tests/fixtures/api/user/get/$.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/get/$.sql)
- [tests/fixtures/api/user/get/$.output.json](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/get/$.output.json)

## What to notice

1. The parameter header `--($args.id integer)--` is the input model. There is no `$.input.json`.
2. The query binds with `{{$args.id}}`.
3. `partition_by` plus `parent_rows` collapses join fan-out into nested `roles`. There is no child SQL file.

Sample result for `id=1`:

```json
{
  "id": 1,
  "name": "admin",
  "roles": [
    { "id": 1, "name": "Administrator" },
    { "id": 2, "name": "User" }
  ]
}
```

The flat join is two rows for user 1. `partition_by: user_id` keeps one object. `roles` with `parent_rows: true` nests those rows.

Omitting `id` removes `optional(u.user_id = {{$args.id}})` and returns the active user that matches the rest of the `WHERE`.

## Exercise

Run `yaal explain user/get` and `yaal explain user/get --arg id=1`. Confirm the optional predicate is gone in the first explain and present, with one bind, in the second.

Next: [Subtractive filters](04-subtractive-filters.md).
