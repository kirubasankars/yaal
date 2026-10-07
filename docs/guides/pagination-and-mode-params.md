# Pagination and `$mode=params`

**Problem:** return a page of rows plus the total count, without a second round trip in application code.

## SQL

`$.paging.sql` (fixture `user/page`):

```sql
--($args.page integer, $args.page_size integer, $params.total_count integer)--

SELECT
    'params' AS "$mode",
    COUNT(*) AS total_count
FROM users
WHERE active = 1

--sql--

SELECT
    {{$args.page}} AS page,
    {{$args.page_size}} AS page_size,
    {{$params.total_count}} AS total_count
```

The first twig is not shaped. Its columns are copied onto `$params`. The second twig binds `total_count`. `$.data.sql` pages users, then joins roles. `$.output.json` nests `data.roles` with `parent_rows`.

## Commands

```bash
yaal query user/page --arg page=1 --arg page_size=1
yaal explain user/page --arg page=1 --arg page_size=10
```

Explain prints **one entry per twig**. Query returns:

```json
{
  "paging": { "page": 1, "page_size": 1, "total_count": 2 },
  "data": [ { "id": 1, "name": "admin", "roles": [ "..."] } ]
}
```

`$mode` values other than `params`: [Mode rows](../reference/mode-rows.md). Namespaces: [Parameter namespaces](../concepts/parameter-namespaces.md).
