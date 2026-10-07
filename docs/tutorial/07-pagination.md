# Pagination

**Goal:** return `{ paging, data }` for a list API.

## Commands

```bash
yaal query user/page --arg page=1 --arg page_size=1
yaal explain user/page --arg page=1 --arg page_size=10
```

## Files to open

- [tests/fixtures/api/user/page/$.paging.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/page/$.paging.sql)
- [tests/fixtures/api/user/page/$.data.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/page/$.data.sql)
- [tests/fixtures/api/user/page/$.output.json](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/page/$.output.json)

## Three ideas

1. **Sibling branches.** There is no trunk `$.sql`. `$.paging.sql` and `$.data.sql` become JSON keys `paging` and `data`.
2. **`$mode=params`.** The first twig runs `COUNT(*)` and copies `total_count` onto `$params`. The next twig reads `{{$params.total_count}}`.
3. **Page parents, then join.** `LIMIT` / `OFFSET` apply to users inside a subquery, then roles are joined. Page size is users, not join rows.

`page=1` and `page_size=1` returns one user (`admin`) with both roles, and `total_count` of 2.

Other `$mode` values (`error`, `break`, `json`): [Mode rows](../reference/mode-rows.md).

## Exercise

Query `user/page` with `page_size=1` for page 1 and page 2. Confirm each page has one user and the same `total_count`.

Next: [Real SQL](08-real-sql.md).
