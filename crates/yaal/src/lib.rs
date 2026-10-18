// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

//! Yaal compiles a SQL string with Yaal syntax and a parameter object into
//! dialect SQL plus ordered bind values.

mod compile;
mod desugar;
mod error;
mod header;
mod lex;
mod token;

use serde::Serialize;
use serde_json::{Map, Value};

pub use crate::compile::Placeholder;
pub use crate::error::CompileError;

#[derive(Clone, Debug)]
pub struct CompileOptions {
    pub placeholder: Placeholder,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            placeholder: Placeholder::Question,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BoundParam {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CompiledStatement {
    pub sql: String,
    pub parameters: Vec<BoundParam>,
}

impl CompiledStatement {
    /// Expanded bind values in placeholder order.
    pub fn values(&self) -> impl Iterator<Item = &serde_json::Value> {
        self.parameters.iter().map(|parameter| &parameter.value)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Compiled {
    pub statements: Vec<CompiledStatement>,
}

/// Compile `sql` with `params` into one statement per `--sql--` section.
pub fn compile(
    sql: &str,
    params: &Map<String, Value>,
    options: &CompileOptions,
) -> Result<Compiled, CompileError> {
    let tokens = lex::lex(sql)?;
    let program = desugar::parse_program(tokens)?;
    let compiled = compile::compile_program(&program, params, options.placeholder)?;
    Ok(Compiled {
        statements: compiled
            .into_iter()
            .map(|(sql, slots)| CompiledStatement {
                sql,
                parameters: slots
                    .into_iter()
                    .map(|slot| BoundParam {
                        name: slot.name,
                        type_name: slot.type_name,
                        value: slot.value,
                    })
                    .collect(),
            })
            .collect(),
    })
}
