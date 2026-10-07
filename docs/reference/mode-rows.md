<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Mode rows

`$mode` is an optional result-column control key. When the first row of a twig includes `$mode`, that result is not shaped as ordinary output.

| `$mode` | Effect |
|---|---|
| `params` | Copy the row’s columns onto `$params`; continue the twig list |
| `error` | Stop; return soft `{"errors": [...]}` |
| `break` | Return these rows as the branch result (`$mode` stripped) |
| `json` | Use the `json` column as the branch result |

Ordinary twigs omit `$mode`. Rows go through `$.output.json`.

## `params`

Copies every column onto `$params`, including `$mode` itself. Later twigs in the same file bind `{{$params.*}}` and declare those names in the header. Fixture: `user/page`.

```sql
SELECT 'params' AS "$mode", COUNT(*) AS total_count FROM users WHERE active = 1
--sql--
SELECT {{$args.page}} AS page, {{$params.total_count}} AS total_count
```

After each twig, providers may set `$params.$last_inserted_id`.

## `error`

Not raised. Same shape as invalid args.

```sql
SELECT 'error' AS "$mode", 1 AS code, 'page out of range' AS message
WHERE {{$args.page}} < 1
```

## `break`

Skips remaining twigs and normal shaping for that branch.

```sql
SELECT 'break' AS "$mode", u.user_id AS id, u.user_name AS name
FROM users u
WHERE u.user_id = {{$args.id}}
```

## `json`

If `json` is a string, it is parsed. Otherwise it is passed through. Shaping is skipped for that branch.

```sql
SELECT 'json' AS "$mode",
       json_group_array(json_object('id', user_id, 'name', user_name)) AS json
FROM users
WHERE active = 1
```
