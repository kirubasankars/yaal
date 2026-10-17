# Safety

Parameter values never become SQL text. Every `{{name}}` and `{{self.field}}` compiles to `?` or `$1`, `$2`, … and the value is returned in `parameters`. `sort()` splices only expressions written in the SQL, chosen by an allowlisted key, and `dir()` accepts only the six fixed directions.

The SQL string itself is trusted. Do not build it from caller input, and execute the compiled statement with the returned binds.

The compiler also rejects SQL that would misalign binds or hide part of the statement:

- A literal `?` outside strings and comments is an error with `?` placeholders. A literal `$1` is an error with numbered placeholders. Use numbered placeholders for the PostgreSQL `?` operator.
- `/* ... */` comments are copied as written, including optimizer hints. A `{{name}}` inside one is not a parameter.
- `--sql--` splits statements only when written exactly. `--sqlite` is an ordinary comment.
- The header must come first. Comments may precede it. A second header is an error.
- Unbalanced parentheses, a stray `}}`, and an unclosed `/*` are errors.
- `optional(...)`, `optional_when(...)`, and the group forms nest at most 32 deep.
- Names are case-insensitive, so `{"id": 1, "ID": 2}` is an error. The same holds for fields in a blob row.
- The bindings reject a NUL character in the SQL, because the C library would stop reading at it.
