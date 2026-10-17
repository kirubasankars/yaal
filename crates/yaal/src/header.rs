// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::collections::BTreeMap;

use crate::error::{err, Result};
use crate::token::is_ident;

pub const METHOD: &str = "query";

#[derive(Clone, Debug)]
pub struct Decl {
    pub name: String,
    pub required: bool,
    pub blob: bool,
}

pub fn parse_header(token_value: &str) -> Result<Option<Vec<Decl>>> {
    if !(token_value.starts_with("--(") && token_value.ends_with(")--")) {
        return Ok(None);
    }
    let inner = &token_value[3..token_value.len() - 3];
    if inner.trim().is_empty() {
        return Err(err(format!("empty parameter header in {METHOD}.sql")));
    }
    let mut params = Vec::new();
    let mut seen = BTreeMap::<String, ()>::new();
    for segment in split_segments(inner)? {
        if segment.trim().is_empty() {
            return Err(err(format!(
                "invalid parameter declaration in {METHOD}.sql"
            )));
        }
        params.push(parse_segment(&segment)?);
        let name = params.last().unwrap().name.clone();
        if seen.contains_key(&name) {
            return Err(err(format!(
                "duplicate parameter {{{{{name}}}}} in {METHOD}.sql"
            )));
        }
        seen.insert(name, ());
    }
    Ok(Some(params))
}

fn parse_segment(segment: &str) -> Result<Decl> {
    let text = segment.trim();
    let (name_raw, rest) = split_name(text).ok_or_else(|| {
        err(format!(
            "invalid parameter declaration '{text}' in {METHOD}.sql"
        ))
    })?;
    let required = name_raw.ends_with('!');
    let name_src = if required {
        &name_raw[..name_raw.len() - 1]
    } else {
        name_raw
    };
    let name = name_src.trim().to_ascii_lowercase();
    if !is_ident(&name) {
        return Err(err(format!(
            "invalid parameter declaration '{text}' in {METHOD}.sql"
        )));
    }
    let rest = rest.trim();
    let blob = if rest.is_empty() {
        false
    } else if rest.eq_ignore_ascii_case("blob") {
        true
    } else {
        return Err(err(format!(
            "invalid parameter declaration '{text}' in {METHOD}.sql"
        )));
    };
    Ok(Decl {
        name,
        required,
        blob,
    })
}

fn split_name(text: &str) -> Option<(&str, &str)> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;
    if i >= bytes.len() {
        return None;
    }
    let first = bytes[i] as char;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    i += 1;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphanumeric() || c == '_' {
            i += 1;
            continue;
        }
        break;
    }
    if i < bytes.len() && bytes[i] == b'!' {
        i += 1;
    }
    if start == i {
        return None;
    }
    Some((&text[start..i], &text[i..]))
}

fn split_segments(inner: &str) -> Result<Vec<String>> {
    Ok(inner
        .split(',')
        .map(|segment| segment.to_string())
        .collect())
}
