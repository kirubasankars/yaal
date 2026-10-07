# Errors and validation

## Raised

Config and I/O failures raise:

| Type | When |
|---|---|
| `DescriptorNotFoundError` | No SQL, or the descriptor cannot be built |
| `UnsupportedDatabaseUrlError` | URL scheme is not supported |
| `PathEscapeError` | Path escapes the API root |
| `YaalError` | Base class |

Compile failures while eliding (partial optional parameters, empty `IN`, bad group rows) also raise (`ValueError` / `TypeError` in Python).

## Soft

Not raised. Invalid args or payload return:

```json
{"errors": [{"message": "..."}]}
```

`$mode=error` uses the same shape. Unknown `sort` / `dir` keys do too. Check for an `errors` key.

## Input validation

Args and payload schemas come from SQL headers (`float` → `number`, `bool` → `boolean`). Soft validation checks types and `required`. A default fills an omitted scalar before that check treats it as missing.

`debug=true` reloads files. It does not change validation.
