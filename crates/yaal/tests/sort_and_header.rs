use serde_json::{json, Map, Value};
use yaal::{compile, CompileOptions};

fn params(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn norm(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sql_of(sql: &str, value: Value) -> String {
    let compiled = compile(sql, &params(value), &CompileOptions::default())
        .unwrap_or_else(|err| panic!("{}", err.message));
    norm(&compiled.statements[0].sql)
}

fn err_has(sql: &str, value: Value, needle: &str) {
    let err = compile(sql, &params(value), &CompileOptions::default()).unwrap_err();
    assert!(
        err.message
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase()),
        "expected {needle:?} in {}",
        err.message
    );
}

const LIST: &str = "--(sort, dir, active)--
select u.user_id, u.user_name from users u
where 1 = 1
  and optional(u.active = {{active}})
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}})
";

#[test]
fn sort_parse_errors() {
    err_has("--(a)--\nselect 1 order by sort()", json!({}), "empty");
    err_has(
        "--(a)--\nselect 1 order by sort(name = x)",
        json!({}),
        "{{param}}",
    );
    err_has(
        "--(a, b)--\nselect 1 order by sort({{a}}, {{b}}, name = x)",
        json!({}),
        "exactly one",
    );
    err_has(
        "--(a)--\nselect 1 order by sort({{a}})",
        json!({}),
        "at least one",
    );
    err_has(
        "--(a)--\nselect 1 order by sort({{a}}, name = x, name = y)",
        json!({}),
        "duplicate",
    );
    err_has("--(a)--\nselect 1 order by dir()", json!({}), "empty");
    err_has(
        "--(a)--\nselect 1 order by dir({{a}}, name = x)",
        json!({}),
        "does not accept",
    );
    err_has(
        "--(a)--\nselect 1 order by sort({{a}}, name = x",
        json!({}),
        "unclosed",
    );
    err_has(
        "--(a)--\nselect 1 order by sort(sort({{a}}, name = x), id = y)",
        json!({}),
        "nested",
    );
    err_has(
        "--(a)--\nselect 1 order by sort({{a}}, bad-key = x)",
        json!({}),
        "word characters",
    );
    err_has(
        "--(a, b)--\nselect 1 order by sort({{a}}, x = y), sort({{b}}, p = q)",
        json!({}),
        "only one dynamic",
    );
    err_has(
        "--(a, b)--\nselect 1 order by sort({{a}}, x = y) foo dir({{b}})",
        json!({}),
        "must not include other terms",
    );
    err_has(
        "--(s)--\nselect 1 order by sort({{s}}, a = x), ,",
        json!({"s": "a"}),
        "empty order by term",
    );
    err_has(
        "--(s)--\nselect * from (select 1 order by sort({{s}}, a = x)) t order by sort({{s}}, a = y)",
        json!({"s": "a"}),
        "more than one sort",
    );
}

#[test]
fn sort_resolution_matrix() {
    let out = sql_of(LIST, json!({"sort": "NAME", "active": null}));
    assert!(out.contains("u.user_name ASC"), "{out}");
    assert!(!out.to_ascii_lowercase().contains("where"), "{out}");

    let out = sql_of(LIST, json!({"sort": "id", "dir": "DESC"}));
    assert!(out.contains("u.user_id DESC"), "{out}");

    let out = sql_of(LIST, json!({"dir": "desc"}));
    assert!(!out.to_ascii_lowercase().contains("order by"), "{out}");

    let out = sql_of(LIST, json!({"sort": "name,id", "dir": "desc"}));
    assert!(out.contains("u.user_name DESC, u.user_id ASC"), "{out}");

    let out = sql_of(LIST, json!({"sort": " name , id ", "dir": " desc , asc "}));
    assert!(out.contains("u.user_name DESC, u.user_id ASC"), "{out}");

    let out = sql_of(LIST, json!({"sort": "id", "dir": "desc_nulls_last"}));
    assert!(out.contains("u.user_id DESC NULLS LAST"), "{out}");

    let out = sql_of(
        LIST,
        json!({"sort": "name,id", "dir": "desc,asc_nulls_last"}),
    );
    assert!(
        out.contains("u.user_name DESC, u.user_id ASC NULLS LAST"),
        "{out}"
    );

    err_has(
        LIST,
        json!({"sort": "id; drop table users"}),
        "unknown sort key",
    );
    err_has(LIST, json!({"sort": ""}), "unknown sort key");
    err_has(LIST, json!({"sort": "   "}), "unknown sort key");
    err_has(LIST, json!({"sort": "name,name"}), "duplicate sort key");
    err_has(
        LIST,
        json!({"sort": "name", "dir": "desc,asc"}),
        "too many dir",
    );
    err_has(
        LIST,
        json!({"sort": "id", "dir": "ascending"}),
        "unknown sort direction",
    );
}

#[test]
fn sort_keeps_static_terms_and_subqueries() {
    let tie = "--(sort, dir)--
select 1 from users u
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}}),
  u.user_id asc
";
    let out = sql_of(tie, json!({"sort": "name", "dir": "desc"}));
    assert_eq!(
        out,
        "select 1 from users u order by u.user_name DESC, u.user_id asc"
    );
    let out = sql_of(tie, json!({}));
    assert_eq!(out, "select 1 from users u order by u.user_id asc");

    let lead = "--(sort, dir)--
select 1 from users u
order by
  u.is_pinned desc,
  sort({{sort}}, name = u.user_name)
  dir({{dir}})
";
    let out = sql_of(lead, json!({"sort": "name", "dir": "desc"}));
    assert!(
        out.contains("order by u.is_pinned desc, u.user_name DESC"),
        "{out}"
    );
    let out = sql_of(lead, json!({}));
    assert_eq!(out, "select 1 from users u order by u.is_pinned desc");

    let nested = "--(s1, d1, s2, d2)--
select * from (
  select * from t1 order by sort({{s1}}, a = x, b = y) dir({{d1}})
) sub
order by sort({{s2}}, c = z, d = w) dir({{d2}})
";
    let out = sql_of(
        nested,
        json!({"s1": "a", "d1": "desc", "s2": "d", "d2": "asc"}),
    );
    assert!(out.contains("order by x DESC"), "{out}");
    assert!(out.contains("order by w ASC"), "{out}");

    let expr = "--(s, d)--
select 1 order by sort({{s}}, name = lower(u.user_name)) dir({{d}})
";
    let out = sql_of(expr, json!({"s": "name", "d": "asc"}));
    assert!(out.contains("lower(u.user_name) ASC"), "{out}");

    let paren = "--(s)--
select 1 order by coalesce(a, b) desc, sort({{s}}, x = y)
";
    let out = sql_of(paren, json!({"s": "x"}));
    assert!(out.contains("coalesce(a, b) desc, y ASC"), "{out}");
}

#[test]
fn lexer_comments_strings_and_headers() {
    let out = sql_of(
        "--(id)--\nselect 1 -- keep going\nfrom t where id = {{id}}",
        json!({"id": 2}),
    );
    assert_eq!(out, "select 1 from t where id = ?");

    err_has("select 'unterminated", json!({}), "unclosed string");
    err_has("--(id)--\nselect {{id}", json!({}), "unclosed");
    err_has("--(id integer", json!({}), "unclosed parameter header");

    let compiled = compile(
        "\n\n--(id!, name, n, ok)--\nselect {{id}}, {{name}}, {{n}}, {{ok}}",
        &params(json!({"id": 9, "name": "a,b", "n": 1.5, "ok": false})),
        &CompileOptions::default(),
    )
    .unwrap();
    let stmt = &compiled.statements[0];
    assert_eq!(norm(&stmt.sql), "select ?, ?, ?, ?");
    let bound: Vec<_> = stmt
        .parameters
        .iter()
        .map(|p| (p.type_name.as_str(), p.value.clone()))
        .collect();
    assert_eq!(
        bound,
        vec![
            ("integer", json!(9)),
            ("string", json!("a,b")),
            ("float", json!(1.5)),
            ("bool", json!(false)),
        ]
    );

    err_has(
        "--(id integer)--\nselect {{id}}",
        json!({"id": 1}),
        "invalid parameter declaration",
    );
    err_has(
        "--(b blob = 'x')--\nselect 1",
        json!({}),
        "invalid parameter declaration",
    );
    err_has(
        "--(a extra)--\nselect 1",
        json!({}),
        "invalid parameter declaration",
    );
    err_has("--(id, id)--\nselect {{id}}", json!({"id": 1}), "duplicate");

    let compiled = compile(
        "--(page)--\nselect {{page}}",
        &params(json!({"page": 8})),
        &CompileOptions::default(),
    )
    .unwrap();
    assert_eq!(compiled.statements[0].parameters[0].value, json!(8));
    assert_eq!(compiled.statements[0].parameters[0].type_name, "integer");
}
