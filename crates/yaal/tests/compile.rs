use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::{json, Map, Value};
use yaal::{compile, CompileOptions, Placeholder};

fn params(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn norm(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn one(sql: &str, value: Value) -> (String, Vec<Value>) {
    let compiled = compile(sql, &params(value), &CompileOptions::default()).unwrap();
    assert_eq!(compiled.statements.len(), 1);
    let stmt = &compiled.statements[0];
    (
        norm(&stmt.sql),
        stmt.parameters.iter().map(|p| p.value.clone()).collect(),
    )
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

#[test]
fn scalar_bind_and_absent_optional() {
    let sql = "--(id)--\nselect * from users u where 1 = 1 and optional(u.id = {{id}})";
    let (out, values) = one(sql, json!({"id": 1}));
    assert_eq!(out, "select * from users u where (u.id = ?)");
    assert_eq!(values, vec![json!(1)]);

    let (out, values) = one(sql, json!({}));
    assert_eq!(out, "select * from users u");
    assert!(values.is_empty());
}

#[test]
fn false_zero_and_empty_string_are_present() {
    let sql = "--(flag, n, name)--\nselect {{flag}}, {{n}}, {{name}}";
    let (_, values) = one(sql, json!({"flag": false, "n": 0, "name": ""}));
    assert_eq!(values, vec![json!(false), json!(0), json!("")]);
}

#[test]
fn omitted_name_stays_absent() {
    let sql = "--(active)--\nselect * from t where optional(active = {{active}})";
    let (out, values) = one(sql, json!({}));
    assert_eq!(out, "select * from t");
    assert!(values.is_empty());

    let (out, _) = one(sql, json!({"active": null}));
    assert_eq!(out, "select * from t");
}

#[test]
fn partial_optional_is_an_error() {
    err_has(
        "--(a, b)--\nselect 1 where optional(a = {{a}} and b = {{b}})",
        json!({"a": 1}),
        "partial parameters",
    );
}

#[test]
fn array_expansion_is_marked_in_sql() {
    let sql = "--(id)--\nselect * from t where optional(id in ({{id}}...))";
    let (out, values) = one(sql, json!({"id": [1, 2]}));
    assert_eq!(out, "select * from t where (id in (?, ?))");
    assert_eq!(values, vec![json!(1), json!(2)]);

    let (out, values) = one(sql, json!({"id": []}));
    assert_eq!(out, "select * from t");
    assert!(values.is_empty());

    err_has(
        "--(id)--\nselect * from t where id in ({{id}}...)",
        json!({"id": []}),
        "empty array",
    );
    err_has(
        "--(id)--\nselect * from t where id in ({{id}}...)",
        json!({"id": 1}),
        "requires a list",
    );
    err_has(
        "--(id)--\nselect * from t where id = {{id}}",
        json!({"id": [1]}),
        "single value",
    );
}

#[test]
fn expand_and_scalar_shapes_conflict() {
    err_has(
        "--(id)--\nselect {{id}} where id in ({{id}}...)",
        json!({"id": [1]}),
        "both as a single value",
    );
}

#[test]
fn required_list_does_not_elide() {
    err_has(
        "--(id!)--\nselect * from t where optional(id in ({{id}}...))",
        json!({"id": []}),
        "required",
    );
}

#[test]
fn header_rejects_a_default() {
    err_has(
        "--(id = 1)--\nselect * from t where id in ({{id}}...)",
        json!({}),
        "invalid parameter declaration",
    );
}

#[test]
fn numbered_placeholders() {
    let sql = "--(a, b)--\nselect {{a}}, {{b}}";
    let compiled = compile(
        sql,
        &params(json!({"a": 1, "b": 2})),
        &CompileOptions {
            placeholder: Placeholder::Numbered,
        },
    )
    .unwrap();
    assert_eq!(norm(&compiled.statements[0].sql), "select $1, $2");
}

#[test]
fn optional_groups_use_self_fields() {
    let sql = "--(pairs blob, active)--\n\
        select * from users u where optional_groups_or({{pairs}}, u.id = {{self.id}} and u.active = {{active}})";
    let (out, values) = one(sql, json!({"pairs": [{"id": 1}, {"id": 2}], "active": 1}));
    assert_eq!(
        out,
        "select * from users u where ((u.id = ? and u.active = ?) or (u.id = ? and u.active = ?))"
    );
    assert_eq!(values, vec![json!(1), json!(1), json!(2), json!(1)]);

    let (out, values) = one(sql, json!({"pairs": [], "active": 1}));
    assert_eq!(out, "select * from users u");
    assert!(values.is_empty());
}

#[test]
fn group_row_list_expands_and_empty_list_errors() {
    let sql = "--(pairs blob)--\n\
        select * from t where optional_groups_and({{pairs}}, id in ({{self.ids}}...))";
    let (out, values) = one(sql, json!({"pairs": [{"ids": [1, 2]}, {"ids": [3]}]}));
    assert_eq!(
        out,
        "select * from t where ((id in (?, ?)) and (id in (?)))"
    );
    assert_eq!(values, vec![json!(1), json!(2), json!(3)]);

    err_has(sql, json!({"pairs": [{"ids": []}]}), "empty array");
    err_has(
        "--(pairs blob)--\nselect * from t where optional_groups_or({{pairs}}, id = {{self.id}})",
        json!({"pairs": [{"id": null}]}),
        "missing optional_groups field",
    );
}

#[test]
fn self_outside_a_group_is_an_error() {
    err_has(
        "--(id)--\nselect {{self.id}}",
        json!({"id": 1}),
        "only valid inside",
    );
}

#[test]
fn optional_when_gates_on_condition() {
    let sql = "--(apply, id)--\nselect * from t where optional_when({{apply}}, id = {{id}})";
    let (kept, values) = one(sql, json!({"apply": false, "id": 4}));
    assert_eq!(kept, "select * from t where (id = ?)");
    assert_eq!(values, vec![json!(4)]);
    let (dropped, _) = one(sql, json!({"apply": null, "id": 4}));
    assert_eq!(dropped, "select * from t");
    err_has(
        "--(apply, id)--\nselect * from t where optional_when({{apply}}..., id = {{id}})",
        json!({"apply": true, "id": 1}),
        "optional_when condition",
    );
}

#[test]
fn sort_and_dir() {
    let sql = "--(sort, dir)--\n\
        select * from users u order by sort({{sort}}, name = u.user_name, id = u.user_id) dir({{dir}}), u.user_id asc";
    let (out, _) = one(sql, json!({"sort": "name,id", "dir": "desc,asc"}));
    assert_eq!(
        out,
        "select * from users u order by u.user_name DESC, u.user_id ASC, u.user_id asc"
    );
    let (sql, _) = one(
        "--(sort)--\nselect * from users u order by sort({{sort}}, name = u.user_name)",
        json!({"sort": null}),
    );
    assert_eq!(sql, "select * from users u");
    err_has(
        "--(sort)--\nselect * from users u order by sort({{sort}}, name = u.user_name)",
        json!({"sort": "nope"}),
        "unknown sort key",
    );
}

#[test]
fn sql_separator_compiles_each_statement() {
    let sql = "--(id)--\nselect {{id}}\n--sql--\nselect {{id}}";
    let compiled = compile(sql, &params(json!({"id": 7})), &CompileOptions::default()).unwrap();
    assert_eq!(compiled.statements.len(), 2);
    assert_eq!(norm(&compiled.statements[0].sql), "select ?");
    assert_eq!(norm(&compiled.statements[1].sql), "select ?");
}

#[test]
fn header_rejects_array_types_and_bad_headers() {
    err_has(
        "--(id integer[])--\nselect {{id}}",
        json!({}),
        "invalid parameter declaration",
    );
    err_has("-- (id)--\nselect 1", json!({}), "without space");
    err_has(
        "--(id! = 1)--\nselect {{id}}",
        json!({}),
        "invalid parameter declaration",
    );
}

#[test]
fn long_form_null_or_elides() {
    let sql = "--(id)--\nselect * from t where ({{id}} is null or id = {{id}})";
    let (out, values) = one(sql, json!({"id": 3}));
    assert_eq!(out, "select * from t where (id = ?)");
    assert_eq!(values, vec![json!(3)]);
    let (out, values) = one(sql, json!({}));
    assert_eq!(out, "select * from t");
    assert!(values.is_empty());
}

#[test]
fn expand_shape_is_file_wide() {
    err_has(
        "--(id)--\nselect {{id}}\n--sql--\nselect {{id}}...",
        json!({"id": 1}),
        "both as a single value",
    );
}

#[test]
fn golden_compile_cases() {
    #[derive(Deserialize)]
    struct Case {
        name: String,
        sql: String,
        params: Map<String, Value>,
        expect_sql: Option<String>,
        expect_param_names: Option<Vec<String>>,
        expect_error_contains: Option<String>,
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/sql_compile/cases.json");
    let cases: Vec<Case> = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert!(cases.len() > 100, "golden cases missing");
    let mut failures = Vec::new();
    for case in cases {
        let result = compile(&case.sql, &case.params, &CompileOptions::default());
        if let Some(needle) = &case.expect_error_contains {
            match result {
                Err(err)
                    if err
                        .message
                        .to_ascii_lowercase()
                        .contains(&needle.to_ascii_lowercase()) => {}
                Err(err) => failures.push(format!(
                    "{}: expected {needle:?} in {}",
                    case.name, err.message
                )),
                Ok(_) => failures.push(format!(
                    "{}: expected error {needle:?}, compiled",
                    case.name
                )),
            }
            continue;
        }
        match result {
            Err(err) => failures.push(format!("{}: {}", case.name, err.message)),
            Ok(compiled) => {
                if compiled.statements.len() != 1 {
                    failures.push(format!(
                        "{}: {} statements",
                        case.name,
                        compiled.statements.len()
                    ));
                    continue;
                }
                let stmt = &compiled.statements[0];
                let got = norm(&stmt.sql);
                let expect = norm(case.expect_sql.as_deref().unwrap_or(""));
                if got != expect {
                    failures.push(format!("{}:\n  got {got}\n  exp {expect}", case.name));
                }
                let names: Vec<_> = stmt
                    .parameters
                    .iter()
                    .map(|p| p.name.clone())
                    .collect::<Vec<_>>();
                let expect_names = case.expect_param_names.unwrap_or_default();
                if names != expect_names {
                    failures.push(format!(
                        "{} names:\n  got {names:?}\n  exp {expect_names:?}",
                        case.name
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn fixtures_cover_the_main_forms() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/sql");
    let sql = fs::read_to_string(root.join("list.sql")).unwrap();
    let (out, values) = one(&sql, json!({"active": 1, "sort": "name", "dir": "desc"}));
    assert!(out.contains("u.active = ?"), "{out}");
    assert!(out.contains("u.user_name DESC"), "{out}");
    assert_eq!(values, vec![json!(1)]);

    let sql = fs::read_to_string(root.join("groups.sql")).unwrap();
    let (out, values) = one(&sql, json!({"pairs": [{"id": 1, "ids": [8, 9]}]}));
    assert!(out.contains("in (?, ?)"), "{out}");
    assert_eq!(values, vec![json!(1), json!(8), json!(9)]);
}
