// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

//! C ABI for the Yaal compiler.
//!
//! `yaal_compile` returns a UTF-8 JSON string. Success is
//! `{"statements":[...]}`. Failure is `{"error":"..."}`. The caller frees
//! that string with `yaal_free`.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};

use serde_json::{Map, Value};
use yaal::{compile, CompileOptions, Placeholder};

/// Compile `sql` with the JSON object in `params_json`.
///
/// `placeholder` is `question`, `numbered`, or null. Null `params_json` means `{}`.
/// The returned pointer is never null. Free it with `yaal_free`.
#[no_mangle]
pub extern "C" fn yaal_compile(
    sql: *const c_char,
    params_json: *const c_char,
    placeholder: *const c_char,
) -> *mut c_char {
    let text = catch_unwind(AssertUnwindSafe(|| {
        compile_text(sql, params_json, placeholder)
    }))
    .unwrap_or_else(|_| error_json("internal compiler error"));
    match CString::new(text) {
        Ok(value) => value.into_raw(),
        Err(_) => CString::new(error_json("compile result contained a null byte"))
            .unwrap()
            .into_raw(),
    }
}

/// Free a string returned by `yaal_compile`. Null is a no-op.
#[no_mangle]
pub extern "C" fn yaal_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    drop(unsafe { CString::from_raw(ptr) });
}

fn compile_text(
    sql: *const c_char,
    params_json: *const c_char,
    placeholder: *const c_char,
) -> String {
    match compile_inner(sql, params_json, placeholder) {
        Ok(value) => value,
        Err(message) => error_json(&message),
    }
}

fn compile_inner(
    sql: *const c_char,
    params_json: *const c_char,
    placeholder: *const c_char,
) -> Result<String, String> {
    let sql = c_str(sql, "sql")?;
    let params = if params_json.is_null() {
        Map::new()
    } else {
        let text = c_str(params_json, "params")?;
        match serde_json::from_str::<Value>(text).map_err(|err| format!("params: {err}"))? {
            Value::Object(map) => map,
            _ => return Err("params must be a JSON object".to_string()),
        }
    };
    let placeholder = if placeholder.is_null() {
        Placeholder::Question
    } else {
        match c_str(placeholder, "placeholder")? {
            "" | "question" | "?" => Placeholder::Question,
            "numbered" | "$" => Placeholder::Numbered,
            other => {
                return Err(format!(
                    "unknown placeholder {other:?}; use question or numbered"
                ))
            }
        }
    };
    let compiled =
        compile(sql, &params, &CompileOptions { placeholder }).map_err(|err| err.message)?;
    serde_json::to_string(&compiled).map_err(|err| err.to_string())
}

fn c_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{what} is null"));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| format!("{what} is not utf-8"))
}

fn error_json(message: &str) -> String {
    serde_json::json!({ "error": message }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(sql: &str, params: Option<&str>, placeholder: Option<&str>) -> String {
        let sql = CString::new(sql).unwrap();
        let params = params.map(|value| CString::new(value).unwrap());
        let placeholder = placeholder.map(|value| CString::new(value).unwrap());
        let ptr = yaal_compile(
            sql.as_ptr(),
            params
                .as_ref()
                .map(|value| value.as_ptr())
                .unwrap_or(std::ptr::null()),
            placeholder
                .as_ref()
                .map(|value| value.as_ptr())
                .unwrap_or(std::ptr::null()),
        );
        assert!(!ptr.is_null());
        let text = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        yaal_free(ptr);
        text
    }

    #[test]
    fn compile_returns_statements_json() {
        let text = call("--(id)--\nselect {{id}}", Some(r#"{"id":7}"#), None);
        let value: Value = serde_json::from_str(&text).unwrap();
        assert!(value.get("error").is_none(), "{text}");
        assert_eq!(value["statements"][0]["parameters"][0]["value"], 7);
    }

    #[test]
    fn numbered_placeholder_and_compile_error() {
        let text = call(
            "--(id)--\nselect {{id}}",
            Some(r#"{"id":7}"#),
            Some("numbered"),
        );
        assert!(text.contains("$1"), "{text}");

        let text = call(
            "--(id)--\nselect {{id}}",
            Some(r#"{"id":7,"extra":1}"#),
            None,
        );
        assert!(text.contains("unknown parameter"), "{text}");
    }

    #[test]
    fn rejects_bad_input_without_crashing() {
        let sql = CString::new("select 1").unwrap();
        let ptr = yaal_compile(std::ptr::null(), std::ptr::null(), std::ptr::null());
        let text = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        yaal_free(ptr);
        assert!(text.contains("sql is null"), "{text}");

        let text = call("select 1", Some("[]"), None);
        assert!(text.contains("JSON object"), "{text}");

        let text = call("select 1", Some("{}"), Some("nope"));
        assert!(text.contains("unknown placeholder"), "{text}");

        yaal_free(std::ptr::null_mut());
        let _ = sql;
    }
}
