// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

//! WebAssembly entry for the Yaal compiler.
//!
//! `compile` returns the same JSON as `yaal_compile`: `{"statements":[...]}`
//! on success and `{"error":"..."}` on failure.

use std::panic::{catch_unwind, AssertUnwindSafe};

use serde_json::{Map, Value};
use wasm_bindgen::prelude::*;
use yaal::compile as compile_sql;
use yaal::{CompileOptions, Placeholder};

/// Compile `sql` with the JSON object in `params_json`.
///
/// `placeholder` is `question` or `numbered`. An empty `params_json` means `{}`.
#[wasm_bindgen]
pub fn compile(sql: &str, params_json: &str, placeholder: &str) -> String {
    let text = catch_unwind(AssertUnwindSafe(|| {
        compile_text(sql, params_json, placeholder)
    }))
    .unwrap_or_else(|_| error_json("internal compiler error"));
    text
}

fn compile_text(sql: &str, params_json: &str, placeholder: &str) -> String {
    match compile_inner(sql, params_json, placeholder) {
        Ok(value) => value,
        Err(message) => error_json(&message),
    }
}

fn compile_inner(sql: &str, params_json: &str, placeholder: &str) -> Result<String, String> {
    let params = if params_json.is_empty() {
        Map::new()
    } else {
        match serde_json::from_str::<Value>(params_json).map_err(|err| format!("params: {err}"))? {
            Value::Object(map) => map,
            _ => return Err("params must be a JSON object".to_string()),
        }
    };
    let placeholder = match placeholder {
        "" | "question" | "?" => Placeholder::Question,
        "numbered" | "$" => Placeholder::Numbered,
        other => {
            return Err(format!(
                "unknown placeholder {other:?}; use question or numbered"
            ))
        }
    };
    let compiled =
        compile_sql(sql, &params, &CompileOptions { placeholder }).map_err(|err| err.message)?;
    serde_json::to_string(&compiled).map_err(|err| err.to_string())
}

fn error_json(message: &str) -> String {
    serde_json::json!({ "error": message }).to_string()
}
