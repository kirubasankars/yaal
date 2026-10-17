# sort and dir

`sort()` splices an allowlisted expression. `dir()` supplies the direction. The client never sends SQL text.

```sql
--(sort, dir)--
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}}),
  u.user_id asc
```

`sort` is one key or a comma-separated list (`name,id`). `dir` matches those keys by position. Missing directions are `asc`. Allowed directions are `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, and `desc_nulls_last`.

An omitted or null `sort` removes that dynamic term. There is no header default. Static terms in the same `ORDER BY` stay. Each `ORDER BY` has at most one dynamic term, and that term is only `sort()` plus an optional `dir()`. `...` is not allowed on either argument.
