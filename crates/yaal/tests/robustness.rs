// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde_json::{json, Map, Value};
use yaal::{compile, CompileOptions, Placeholder};

fn params(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn compile_with(sql: &str, value: Value, placeholder: Placeholder) -> Result<String, String> {
    compile(sql, &params(value), &CompileOptions { placeholder })
        .map(|compiled| {
            compiled
                .statements
                .iter()
                .map(|s| s.sql.split_whitespace().collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
                .join(" ; ")
        })
        .map_err(|e| e.message)
}

fn rejects(sql: &str, value: Value, placeholder: Placeholder, needle: &str) {
    let message = compile_with(sql, value, placeholder).unwrap_err();
    assert!(
        message.contains(needle),
        "expected {needle:?} in {message:?}"
    );
}

#[test]
fn stray_closing_braces_are_an_error() {
    rejects(
        "--(id)--\nselect }} from t",
        json!({}),
        Placeholder::Question,
        "unexpected }}",
    );
}

#[test]
fn block_comments_are_opaque() {
    let sql = "--(id)--\nselect /*+ hint */ * from t /* {{id}} ( ? */ where a = {{id}}";
    let out = compile_with(sql, json!({"id": 1}), Placeholder::Question).unwrap();
    assert_eq!(
        out,
        "select /*+ hint */ * from t /* {{id}} ( ? */ where a = ?"
    );
    let compiled = compile(sql, &params(json!({"id": 1})), &CompileOptions::default()).unwrap();
    assert_eq!(compiled.statements[0].parameters.len(), 1);
    rejects(
        "--(id)--\nselect 1 /* open",
        json!({}),
        Placeholder::Question,
        "unclosed /* comment",
    );
}

#[test]
fn comment_banner_may_precede_the_header() {
    let out = compile_with(
        "/* report */\n--(id)--\nselect {{id}}",
        json!({"id": 1}),
        Placeholder::Question,
    )
    .unwrap();
    assert_eq!(out, "/* report */ select ?");
}

#[test]
fn only_exact_sql_separator_splits() {
    let sql = "--(id)--\nselect * from t where a = {{id}}\n--sqlite note\nand b = 1";
    let out = compile_with(sql, json!({"id": 1}), Placeholder::Question).unwrap();
    assert_eq!(out, "select * from t where a = ? and b = 1");
}

#[test]
fn header_must_come_first() {
    rejects(
        "--(id)--\nselect {{id}}\n--(x)--\nselect 2",
        json!({"id": 1}),
        Placeholder::Question,
        "must come first",
    );
}

#[test]
fn ordinary_comment_may_start_with_a_paren() {
    let out = compile_with(
        "--(id)--\nselect * from t\n-- (legacy) note\nwhere a = {{id}}",
        json!({"id": 1}),
        Placeholder::Question,
    )
    .unwrap();
    assert_eq!(out, "select * from t where a = ?");
}

#[test]
fn literal_placeholders_are_rejected() {
    rejects(
        "--(id)--\nselect * from t where a = {{id}} and b ? 'k'",
        json!({"id": 1}),
        Placeholder::Question,
        "literal ?",
    );
    rejects(
        "--(id)--\nselect * from t where a = {{id}} and b = $1",
        json!({"id": 1}),
        Placeholder::Numbered,
        "literal $1",
    );
    rejects(
        "--(s)--\nselect * from t order by sort({{s}}, a = x ? 1)",
        json!({"s": "a"}),
        Placeholder::Question,
        "literal ?",
    );
    let out = compile_with(
        "--(id)--\nselect * from t where a = {{id}} and b ? 'k' and c = '$1' and d$1 = 2",
        json!({"id": 1}),
        Placeholder::Numbered,
    )
    .unwrap();
    assert_eq!(
        out,
        "select * from t where a = $1 and b ? 'k' and c = '$1' and d$1 = 2"
    );
    let out = compile_with(
        "--(id)--\nselect '?' from t where a = {{id}}",
        json!({"id": 1}),
        Placeholder::Question,
    )
    .unwrap();
    assert_eq!(out, "select '?' from t where a = ?");
}

#[test]
fn parentheses_must_balance() {
    rejects(
        "--(id)--\nselect * from t where (a = {{id}}))",
        json!({"id": 1}),
        Placeholder::Question,
        "unbalanced )",
    );
    rejects(
        "--(id)--\nselect * from t where ((a = {{id}})",
        json!({"id": 1}),
        Placeholder::Question,
        "unclosed (",
    );
}

#[test]
fn parameter_keys_differing_by_case_are_rejected() {
    rejects(
        "--(id)--\nselect {{id}}",
        json!({"id": 1, "ID": 2}),
        Placeholder::Question,
        "given more than once",
    );
    rejects(
        "--(pairs blob)--\nselect * from t where optional_groups_or({{pairs}}, id = {{self.id}})",
        json!({"pairs": [{"id": 1, "ID": 2}]}),
        Placeholder::Question,
        "given more than once",
    );
}

#[test]
fn optional_nesting_is_bounded() {
    let deep = |depth: usize| {
        format!(
            "--(a)--\nselect * from t where {}x = {{{{a}}}}{}",
            "optional(".repeat(depth),
            ")".repeat(depth)
        )
    };
    assert!(compile_with(&deep(32), json!({"a": 1}), Placeholder::Question).is_ok());
    rejects(
        &deep(33),
        json!({"a": 1}),
        Placeholder::Question,
        "nested more than 32 levels",
    );
    rejects(
        &deep(100_000),
        json!({"a": 1}),
        Placeholder::Question,
        "nested more than 32 levels",
    );
}

const FRAGMENTS: &[&str] = &[
    "optional(",
    "optional_when(",
    "optional_groups_or(",
    "optional_groups_and(",
    "sort(",
    "dir(",
    "(",
    ")",
    ",",
    " and ",
    " or ",
    " where ",
    " having ",
    " order by ",
    " group by ",
    " limit 1",
    "{{a}}",
    "{{b}}",
    "{{ids}}...",
    "{{p}}",
    "{{self.x}}",
    "{{self.ys}}...",
    "{{s}}",
    "{{d}}",
    " is null or ",
    "1 = 1",
    " = ",
    "x",
    "'str'",
    "\"id\"",
    "\n",
    "--sql--",
    "-- note\n",
    "/* c */",
    "}}",
    "{{",
    "'",
    "?",
    "$1",
    "name = u.name",
    " select ",
    " from t ",
];

const HEADERS: &[&str] = &[
    "--(a, b, ids, p blob, s, d)--\n",
    "--(a!, b, ids, p blob, s, d)--\n",
    "--(a)--\n",
    "",
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn params_for(rng: &mut Rng) -> Map<String, Value> {
    let choices = [
        json!({}),
        json!({"a": 1, "b": "x", "ids": [1, 2], "p": [{"x": 1, "ys": [1]}], "s": "name", "d": "desc"}),
        json!({"a": null, "ids": [], "p": []}),
        json!({"a": true, "p": "[{\"x\": 2, \"ys\": [3, 4]}]"}),
        json!({"a": [1], "b": {"k": 1}, "p": [1]}),
        json!({"s": "nope", "d": "sideways"}),
    ];
    choices[rng.below(choices.len())]
        .as_object()
        .unwrap()
        .clone()
}

fn compiles_within(sql: String, params: Map<String, Value>, placeholder: Placeholder) {
    let (tx, rx) = mpsc::channel();
    let shown = sql.clone();
    thread::spawn(move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = compile(&sql, &params, &CompileOptions { placeholder });
        }));
        let _ = tx.send(outcome.is_ok());
    });
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(true) => {}
        Ok(false) => panic!("compile panicked on {shown:?}"),
        Err(_) => panic!("compile did not finish on {shown:?}"),
    }
}

fn expr(rng: &mut Rng, depth: usize) -> String {
    const LEAVES: &[&str] = &[
        "x = {{a}}",
        "x = {{b}}",
        "x in ({{ids}}...)",
        "({{a}} is null or x = {{a}})",
        "y = {{self.x}}",
        "z in ({{self.ys}}...)",
        "1 = 1",
        "w = 's'",
        "/* c */ v = 1",
    ];
    if depth == 0 || rng.below(3) == 0 {
        return LEAVES[rng.below(LEAVES.len())].to_string();
    }
    let inner = expr(rng, depth - 1);
    match rng.below(7) {
        0 => format!("optional({inner})"),
        1 => format!("optional_when({{{{a}}}}, {inner})"),
        2 => format!("optional_groups_or({{{{p}}}}, {inner})"),
        3 => format!("optional_groups_and({{{{p}}}}, {inner})"),
        4 => format!("({inner})"),
        5 => format!("{inner} and {}", expr(rng, depth - 1)),
        _ => format!("{inner} or {}", expr(rng, depth - 1)),
    }
}

fn query(rng: &mut Rng) -> String {
    const TAILS: &[&str] = &[
        "",
        " order by sort({{s}}, name = u.name, id = u.id) dir({{d}})",
        " order by id",
        "\n--sql--\nselect {{a}}",
    ];
    format!(
        "--(a, b, ids, p blob, s, d)--\nselect * from t where {}{}",
        expr(rng, 4),
        TAILS[rng.below(TAILS.len())]
    )
}

fn placeholders_match_binds(sql: &str, params: &Map<String, Value>, placeholder: Placeholder) {
    let Ok(compiled) = compile(sql, params, &CompileOptions { placeholder }) else {
        return;
    };
    for statement in &compiled.statements {
        let count = statement.parameters.len();
        match placeholder {
            Placeholder::Question => {
                assert_eq!(
                    statement.sql.matches('?').count(),
                    count,
                    "{sql:?} compiled to {:?}",
                    statement.sql
                );
            }
            Placeholder::Numbered => {
                for n in 1..=count {
                    assert!(
                        statement.sql.contains(&format!("${n}")),
                        "{sql:?} compiled to {:?}",
                        statement.sql
                    );
                }
                assert!(!statement.sql.contains(&format!("${}", count + 1)));
            }
        }
    }
}

#[test]
fn random_queries_bind_every_placeholder() {
    let cases = std::env::var("YAAL_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4000);
    let seed = std::env::var("YAAL_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x2545_f491_4f6c_dd1d);
    let mut rng = Rng(seed);
    for _ in 0..cases {
        let sql = query(&mut rng);
        let params = params_for(&mut rng);
        let placeholder = if rng.below(2) == 0 {
            Placeholder::Question
        } else {
            Placeholder::Numbered
        };
        compiles_within(sql.clone(), params.clone(), placeholder);
        placeholders_match_binds(&sql, &params, placeholder);
    }
}

#[test]
fn random_fragments_never_panic_or_hang() {
    let cases = std::env::var("YAAL_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4000);
    let seed = std::env::var("YAAL_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x9e37_79b9_7f4a_7c15);
    let mut rng = Rng(seed);
    for _ in 0..cases {
        let mut sql = HEADERS[rng.below(HEADERS.len())].to_string();
        for _ in 0..1 + rng.below(30) {
            sql.push_str(FRAGMENTS[rng.below(FRAGMENTS.len())]);
        }
        let placeholder = if rng.below(2) == 0 {
            Placeholder::Question
        } else {
            Placeholder::Numbered
        };
        compiles_within(sql, params_for(&mut rng), placeholder);
    }
}
