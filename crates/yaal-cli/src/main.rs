// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::env;
use std::fs;
use std::io::{self, IsTerminal, Read};
use std::process::ExitCode;

use serde_json::{Map, Value};
use yaal::{compile, CompileOptions, Placeholder};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_default();
    if command != "compile" {
        return Err(usage());
    }
    let mut sql_path = None;
    let mut params_path = None;
    let mut pairs = Vec::new();
    let mut placeholder = Placeholder::Question;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sql" => sql_path = Some(need(&mut args, "--sql")?),
            "--params" => params_path = Some(need(&mut args, "--params")?),
            "--param" => pairs.push(need(&mut args, "--param")?),
            "--placeholder" => {
                placeholder = match need(&mut args, "--placeholder")?.as_str() {
                    "question" | "?" => Placeholder::Question,
                    "numbered" | "$" => Placeholder::Numbered,
                    other => {
                        return Err(format!(
                            "unknown placeholder {other:?}; use question or numbered"
                        ))
                    }
                }
            }
            "--help" | "-h" => {
                println!("{}", usage());
                return Ok(());
            }
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }

    let sql = match sql_path {
        Some(path) => fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?,
        None if io::stdin().is_terminal() => {
            return Err(format!(
                "SQL was not provided. Pass --sql, or pipe SQL on stdin.\n{}",
                usage()
            ));
        }
        None => {
            let mut buf = String::new();
            io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| format!("read stdin: {e}"))?;
            buf
        }
    };
    let mut params = Map::new();
    if let Some(path) = params_path {
        let text = fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
        let value: Value = serde_json::from_str(&text).map_err(|e| format!("parse {path}: {e}"))?;
        let Some(obj) = value.as_object() else {
            return Err(format!("{path} must be a JSON object"));
        };
        params = obj.clone();
    }
    for pair in pairs {
        let (key, raw) = pair
            .split_once('=')
            .ok_or_else(|| format!("--param {pair} must be name=value"))?;
        params.insert(key.to_string(), parse_param_value(raw));
    }
    let compiled =
        compile(&sql, &params, &CompileOptions { placeholder }).map_err(|e| e.message)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&compiled).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn parse_param_value(raw: &str) -> Value {
    let text = raw.trim();
    match text {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        "null" => return Value::Null,
        _ => {}
    }
    if text.starts_with('[') || text.starts_with('{') || text.starts_with('"') {
        if let Ok(value) = serde_json::from_str(text) {
            return value;
        }
    }
    if let Ok(n) = text.parse::<i64>() {
        return Value::from(n);
    }
    if text.contains('.') {
        if let Ok(n) = text.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(n) {
                return Value::Number(n);
            }
        }
    }
    Value::String(text.to_string())
}

fn need(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn usage() -> String {
    "\
yaal compile [--sql file.sql] [--params file.json] [--param name=value]... [--placeholder question|numbered]

Reads SQL from --sql, or from stdin when --sql is omitted and stdin is not a terminal.
Prints JSON: {\"statements\":[{\"sql\",\"parameters\":[{\"name\",\"type\",\"value\"}]}]}.
"
    .to_string()
}
