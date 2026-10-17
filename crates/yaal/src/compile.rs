// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::desugar::{split_order_terms, Program, Stmt};
use crate::error::{err, Result};
use crate::header::Decl;
use crate::lex::braced;
use crate::token::{Token, TokenKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placeholder {
    Question,
    Numbered,
}

impl Default for Placeholder {
    fn default() -> Self {
        Self::Question
    }
}

struct Ph {
    style: Placeholder,
    n: u32,
}

impl Ph {
    fn next(&mut self) -> String {
        self.n += 1;
        match self.style {
            Placeholder::Question => "?".to_string(),
            Placeholder::Numbered => format!("${}", self.n),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Slot {
    pub name: String,
    pub type_name: String,
    pub value: Value,
}

struct Ctx<'a> {
    decls: &'a BTreeMap<String, Decl>,
    values: &'a BTreeMap<String, Value>,
    nulls: &'a BTreeSet<String>,
    blobs: &'a BTreeMap<String, Vec<Map<String, Value>>>,
    sort_map: &'a BTreeMap<String, Option<String>>,
    ph: Ph,
}

const DIR_VOCAB: &[(&str, &str)] = &[
    ("asc", "ASC"),
    ("desc", "DESC"),
    ("asc_nulls_first", "ASC NULLS FIRST"),
    ("asc_nulls_last", "ASC NULLS LAST"),
    ("desc_nulls_first", "DESC NULLS FIRST"),
    ("desc_nulls_last", "DESC NULLS LAST"),
];

pub fn compile_program(
    program: &Program,
    params: &Map<String, Value>,
    placeholder: Placeholder,
) -> Result<Vec<(String, Vec<Slot>)>> {
    let values = effective_values(&program.decls, params)?;
    check_required(&program.decls, &values, &program.expand_names)?;
    let mut out = Vec::new();
    for stmt in &program.stmts {
        let nulls = nulls_for(stmt, &program.decls, &values, &program.expand_names);
        let blobs = blobs_for(stmt, &values, &nulls)?;
        let sort_map = resolve_sort(stmt, &values)?;
        let mut ctx = Ctx {
            decls: &program.decls,
            values: &values,
            nulls: &nulls,
            blobs: &blobs,
            sort_map: &sort_map,
            ph: Ph {
                style: placeholder,
                n: 0,
            },
        };
        let (fragments, slots) =
            compile_range(&stmt.content, 0, stmt.content.len(), &mut ctx, None, None)?;
        let sql = cleanup(fragments).join("");
        out.push((sql, slots));
    }
    Ok(out)
}

fn effective_values(
    decls: &BTreeMap<String, Decl>,
    params: &Map<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    let mut lowered = BTreeMap::new();
    for (k, v) in params {
        lowered.insert(k.to_ascii_lowercase(), v.clone());
    }
    let mut values = BTreeMap::new();
    for name in decls.keys() {
        match lowered.get(name) {
            None | Some(Value::Null) => {}
            Some(v) => {
                values.insert(name.clone(), v.clone());
            }
        }
    }
    for (k, _) in &lowered {
        if !decls.contains_key(k) {
            return Err(err(format!("unknown parameter {}", braced(k))));
        }
    }
    Ok(values)
}

fn is_absent(decl: &Decl, value: Option<&Value>, expand: bool) -> bool {
    let Some(value) = value else {
        return true;
    };
    if value.is_null() {
        return true;
    }
    if expand {
        return matches!(value, Value::Array(items) if items.is_empty());
    }
    if decl.blob {
        if let Some(text) = value.as_str() {
            let text = text.trim();
            return text.is_empty() || text == "[]";
        }
        return matches!(value, Value::Array(items) if items.is_empty());
    }
    false
}

fn check_required(
    decls: &BTreeMap<String, Decl>,
    values: &BTreeMap<String, Value>,
    expand_names: &BTreeSet<String>,
) -> Result<()> {
    for (name, decl) in decls {
        if !decl.required {
            continue;
        }
        let expand = expand_names.contains(name);
        if is_absent(decl, values.get(name), expand) {
            return Err(err(format!("{} is required", braced(name))));
        }
    }
    Ok(())
}

fn nulls_for(
    stmt: &Stmt,
    decls: &BTreeMap<String, Decl>,
    values: &BTreeMap<String, Value>,
    expand_names: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut nulls = BTreeSet::new();
    for name in &stmt.nullable {
        let Some(decl) = decls.get(name) else {
            continue;
        };
        if is_absent(decl, values.get(name), expand_names.contains(name)) {
            nulls.insert(name.clone());
        }
    }
    nulls
}

fn blobs_for(
    stmt: &Stmt,
    values: &BTreeMap<String, Value>,
    nulls: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<Map<String, Value>>>> {
    let mut blobs = BTreeMap::new();
    for token in &stmt.content {
        let Some(source) = &token.group_source else {
            continue;
        };
        if nulls.contains(source) || blobs.contains_key(source) {
            continue;
        }
        let value = values.get(source).ok_or_else(|| {
            err(format!(
                "missing blob value for optional_groups {}",
                braced(source)
            ))
        })?;
        blobs.insert(source.clone(), coerce_blob(value, source)?);
    }
    Ok(blobs)
}

fn coerce_blob(value: &Value, source: &str) -> Result<Vec<Map<String, Value>>> {
    let parsed = if let Some(text) = value.as_str() {
        let text = text.trim();
        if text.is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_str::<Value>(text).map_err(|_| {
            err(format!(
                "optional_groups {} must be a JSON array of objects",
                braced(source)
            ))
        })?
    } else {
        value.clone()
    };
    let Some(items) = parsed.as_array() else {
        return Err(err(format!(
            "optional_groups {} must be a JSON array of objects",
            braced(source)
        )));
    };
    let mut rows = Vec::new();
    for item in items {
        let Some(obj) = item.as_object() else {
            return Err(err(format!(
                "optional_groups {} rows must be objects",
                braced(source)
            )));
        };
        rows.push(obj.clone());
    }
    Ok(rows)
}

fn resolve_sort(
    stmt: &Stmt,
    values: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Option<String>>> {
    let mut sort_map = BTreeMap::new();
    for (idx, token) in stmt.content.iter().enumerate() {
        if token.kind != TokenKind::Sort {
            continue;
        }
        let param = &token.sort_param;
        let Some(raw) = values.get(param) else {
            sort_map.insert(param.clone(), None);
            continue;
        };
        let raw_sort = raw
            .as_str()
            .ok_or_else(|| err(format!("sort parameter {} must be a string", braced(param))))?;
        let keys: Vec<String> = raw_sort
            .split(',')
            .map(|k| k.trim().to_ascii_lowercase())
            .collect();
        let mut seen = BTreeSet::new();
        for key in &keys {
            if key.is_empty() {
                return Err(err(format!("unknown sort key: {raw_sort:?}")));
            }
            if !token.sort_choices.contains_key(key) {
                return Err(err(format!("unknown sort key: {key}")));
            }
            if !seen.insert(key.clone()) {
                return Err(err(format!("duplicate sort key: {key}")));
            }
        }
        let dir_param = paired_dir(&stmt.content, idx);
        let raw_dir = dir_param.and_then(|name| values.get(name));
        let mut dirs = Vec::new();
        let raw_dir_text;
        if let Some(raw_dir) = raw_dir {
            raw_dir_text = raw_dir.as_str().map(|s| s.to_string()).ok_or_else(|| {
                err(format!(
                    "dir parameter {} must be a string",
                    braced(dir_param.unwrap_or(""))
                ))
            })?;
            dirs = raw_dir_text
                .split(',')
                .map(|d| d.trim().to_ascii_lowercase())
                .collect();
        } else {
            raw_dir_text = String::new();
        }
        if dirs.len() > keys.len() {
            return Err(err(format!(
                "too many dir values for {} sort key(s): {raw_dir_text}",
                keys.len()
            )));
        }
        while dirs.len() < keys.len() {
            dirs.push("asc".to_string());
        }
        let mut parts = Vec::new();
        for (key, dir) in keys.iter().zip(dirs.iter()) {
            let sql = DIR_VOCAB
                .iter()
                .find(|(k, _)| *k == dir)
                .map(|(_, v)| *v)
                .ok_or_else(|| err(format!("unknown sort direction: {dir}")))?;
            let expr = &token.sort_choices[key];
            parts.push(format!("{expr} {sql}"));
        }
        sort_map.insert(param.clone(), Some(parts.join(", ")));
    }
    Ok(sort_map)
}

fn paired_dir(content: &[Token], sort_index: usize) -> Option<&str> {
    let mut i = sort_index + 1;
    while i < content.len() && content[i].is_ws() {
        i += 1;
    }
    if i < content.len() && content[i].kind == TokenKind::Dir {
        Some(content[i].sort_param.as_str())
    } else {
        None
    }
}

fn compile_range(
    stmt: &[Token],
    start: usize,
    end: usize,
    ctx: &mut Ctx<'_>,
    group_source: Option<&str>,
    group_index: Option<usize>,
) -> Result<(Vec<String>, Vec<Slot>)> {
    let mut tokens: Vec<String> = Vec::new();
    let mut slots = Vec::new();
    let mut skipping: Option<i32> = None;
    let mut skip_or = false;
    let mut idx = start;
    while idx < end {
        let token = &stmt[idx];
        if token.word_eq("order") {
            if let Some((is_order, fragments, next)) = compile_order_by(stmt, idx, end, ctx)? {
                if is_order {
                    if let Some(fragments) = fragments {
                        tokens.extend(fragments);
                    } else {
                        while tokens.last().is_some_and(|t| is_ws_fragment(t)) {
                            tokens.pop();
                        }
                    }
                    idx = next.min(end);
                    continue;
                }
            }
        }
        if token.kind == TokenKind::Brace {
            if let Some(group) = skipping {
                if token.group == group {
                    skipping = None;
                }
                idx += 1;
                continue;
            }
            if token.value == "(" && token.groups_wrapper {
                let close = find_close(stmt, idx).ok_or_else(|| err("unclosed optional(...)"))?;
                if wrapped_groups_elide(stmt, idx + 1, close, ctx) {
                    strip_connector(&mut tokens);
                    idx = close + 1;
                    continue;
                }
            }
            if token.group_source.is_some() {
                if should_elide(token, ctx)? {
                    strip_connector(&mut tokens);
                    let close = find_close(stmt, idx)
                        .ok_or_else(|| err("unclosed optional_groups_*(...)"))?;
                    idx = close + 1;
                    continue;
                }
                let source = token.group_source.clone().unwrap();
                let rows = ctx.blobs.get(&source).cloned().ok_or_else(|| {
                    err(format!(
                        "missing optional_groups count for {}",
                        braced(&source)
                    ))
                })?;
                if rows.is_empty() {
                    strip_connector(&mut tokens);
                    let close = find_close(stmt, idx)
                        .ok_or_else(|| err("unclosed optional_groups_*(...)"))?;
                    idx = close + 1;
                    continue;
                }
                let close =
                    find_close(stmt, idx).ok_or_else(|| err("unclosed optional_groups_*(...)"))?;
                let mut branches = Vec::new();
                let mut branch_slots = Vec::new();
                for (gi, _row) in rows.iter().enumerate() {
                    let (bt, bp) =
                        compile_range(stmt, idx + 1, close, ctx, Some(&source), Some(gi))?;
                    branches.push(format!("({})", bt.join("")));
                    branch_slots.extend(bp);
                }
                let joiner = if token.group_join_and {
                    " and "
                } else {
                    " or "
                };
                let joined = branches.join(joiner);
                if branches.len() > 1 {
                    tokens.push(format!("({joined})"));
                } else {
                    tokens.push(joined);
                }
                slots.extend(branch_slots);
                idx = close + 1;
                continue;
            }
            if should_elide(token, ctx)? {
                strip_connector(&mut tokens);
                skipping = Some(token.group);
                skip_or = false;
                idx += 1;
                continue;
            }
        }
        if skipping.is_some() {
            idx += 1;
            continue;
        }
        if skip_or {
            if is_ws_fragment(&token.value) {
                idx += 1;
                continue;
            }
            if token.word_eq("or") {
                idx += 1;
                continue;
            }
            skip_or = false;
        }
        if token.kind == TokenKind::Sort {
            if let Some(expr) = ctx.sort_map.get(&token.sort_param).and_then(|v| v.as_ref()) {
                tokens.push(expr.clone());
            }
            idx += 1;
            continue;
        }
        if token.kind == TokenKind::Dir {
            idx += 1;
            continue;
        }
        if token.kind == TokenKind::GroupField {
            let source = group_source.ok_or_else(|| {
                err(format!(
                    "{} is only valid inside optional_groups_*(...)",
                    braced(&format!("self.{}", token.name))
                ))
            })?;
            let gi = group_index.unwrap_or(0);
            emit_group_field(token, source, gi, ctx, &mut tokens, &mut slots)?;
            idx += 1;
            continue;
        }
        if token.kind == TokenKind::Parameter {
            if token.nullable_marker {
                skip_or = true;
                idx += 1;
                continue;
            }
            emit_arg(token, ctx, &mut tokens, &mut slots)?;
        } else {
            tokens.push(token.value.clone());
        }
        idx += 1;
    }
    Ok((tokens, slots))
}

fn emit_arg(
    token: &Token,
    ctx: &mut Ctx<'_>,
    tokens: &mut Vec<String>,
    slots: &mut Vec<Slot>,
) -> Result<()> {
    if !ctx.decls.contains_key(&token.name) {
        return Err(err(format!(
            "type missing for {} in the query.sql",
            braced(&token.name)
        )));
    }
    if token.expand {
        let value = ctx
            .values
            .get(&token.name)
            .ok_or_else(|| err(format!("missing array value for {}", braced(&token.name))))?;
        let Some(items) = value.as_array() else {
            return Err(err(format!("{}... requires a list", braced(&token.name))));
        };
        if items.is_empty() {
            return Err(err(format!(
                "empty array for {}; IN list cannot be empty",
                braced(&token.name)
            )));
        }
        let mut phs = Vec::new();
        for item in items {
            phs.push(ctx.ph.next());
            let (type_name, value) = bind_value(item, &token.name)?;
            slots.push(Slot {
                name: token.name.clone(),
                type_name,
                value,
            });
        }
        tokens.push(phs.join(", "));
        return Ok(());
    }
    if ctx.values.get(&token.name).is_some_and(|v| v.is_array()) {
        return Err(err(format!(
            "{} must be a single value",
            braced(&token.name)
        )));
    }
    let (type_name, value) = match ctx.values.get(&token.name) {
        Some(v) => bind_value(v, &token.name)?,
        None => ("string".to_string(), Value::Null),
    };
    tokens.push(ctx.ph.next());
    slots.push(Slot {
        name: token.name.clone(),
        type_name,
        value,
    });
    Ok(())
}

fn emit_group_field(
    token: &Token,
    source: &str,
    gi: usize,
    ctx: &mut Ctx<'_>,
    tokens: &mut Vec<String>,
    slots: &mut Vec<Slot>,
) -> Result<()> {
    let rows = ctx.blobs.get(source).ok_or_else(|| {
        err(format!(
            "missing optional_groups count for {}",
            braced(source)
        ))
    })?;
    let row = rows.get(gi).ok_or_else(|| {
        err(format!(
            "optional_groups row missing for {}",
            braced(&format!("self.{}", token.name))
        ))
    })?;
    let raw = row_get(row, &token.name).ok_or_else(|| {
        err(format!(
            "missing optional_groups field {} in {}",
            braced(&token.name),
            source
        ))
    })?;
    let label = format!("self.{}", token.name);
    if raw.is_null() {
        return Err(err(format!(
            "missing optional_groups field {} in {source}",
            braced(&token.name)
        )));
    }
    if token.expand {
        let Some(items) = raw.as_array() else {
            return Err(err(format!("{}... requires a list", braced(&label))));
        };
        if items.is_empty() {
            return Err(err(format!(
                "empty array for optional_groups field {} in {source}; IN list cannot be empty",
                braced(&token.name)
            )));
        }
        let mut phs = Vec::new();
        for item in items {
            assert_scalar(item, source, &token.name)?;
            phs.push(ctx.ph.next());
            let (type_name, value) = infer_scalar(item)?;
            slots.push(Slot {
                name: label.clone(),
                type_name,
                value,
            });
        }
        tokens.push(phs.join(", "));
        return Ok(());
    }
    if raw.as_array().is_some() {
        return Err(err(format!("{} must be a single value", braced(&label))));
    }
    assert_scalar(raw, source, &token.name)?;
    let (type_name, value) = infer_scalar(raw)?;
    tokens.push(ctx.ph.next());
    slots.push(Slot {
        name: label,
        type_name,
        value,
    });
    Ok(())
}

fn row_get<'a>(row: &'a Map<String, Value>, field: &str) -> Option<&'a Value> {
    if let Some(v) = row.get(field) {
        return Some(v);
    }
    row.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(field))
        .map(|(_, v)| v)
}

fn assert_scalar(value: &Value, source: &str, field: &str) -> Result<()> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Ok(()),
        _ => Err(err(format!(
            "optional_groups field {} in {source} must be a scalar value",
            braced(field)
        ))),
    }
}

fn infer_scalar(value: &Value) -> Result<(String, Value)> {
    match value {
        Value::Bool(b) => Ok(("bool".into(), Value::Bool(*b))),
        Value::Number(n) => {
            if n.as_i64().is_some() || n.as_u64().is_some() {
                Ok(("integer".into(), Value::Number(n.clone())))
            } else {
                Ok(("float".into(), Value::Number(n.clone())))
            }
        }
        Value::String(s) => Ok(("string".into(), Value::String(s.clone()))),
        Value::Null => Ok(("string".into(), Value::Null)),
        _ => Err(err("internal: non-scalar group field")),
    }
}

fn bind_value(value: &Value, name: &str) -> Result<(String, Value)> {
    match value {
        Value::Array(_) | Value::Object(_) => {
            Err(err(format!("{} must be a single value", braced(name))))
        }
        other => infer_scalar(other),
    }
}

fn should_elide(token: &Token, ctx: &Ctx<'_>) -> Result<bool> {
    if let Some(cond) = &token.optional_when {
        if ctx.nulls.contains(cond) {
            return Ok(true);
        }
    }
    if let Some(source) = &token.group_source {
        return Ok(ctx.nulls.contains(source));
    }
    if !token.nullable_parameters.is_empty() {
        let missing: Vec<_> = token
            .nullable_parameters
            .iter()
            .filter(|n| ctx.nulls.contains(*n))
            .cloned()
            .collect();
        if missing.len() == token.nullable_parameters.len() {
            return Ok(true);
        }
        if missing.is_empty() {
            return Ok(false);
        }
        let mut sorted = missing;
        sorted.sort();
        return Err(err(format!(
            "optional(...) requires every parameter to be provided or all omitted; partial parameters: {}",
            sorted.join(", ")
        )));
    }
    if let Some(name) = &token.nullable_parameter {
        return Ok(ctx.nulls.contains(name));
    }
    Ok(false)
}

fn wrapped_groups_elide(stmt: &[Token], start: usize, end: usize, ctx: &Ctx<'_>) -> bool {
    for token in &stmt[start..end] {
        if token.kind != TokenKind::Brace || token.value != "(" {
            continue;
        }
        let Some(source) = &token.group_source else {
            continue;
        };
        if ctx.nulls.contains(source) {
            continue;
        }
        if ctx.blobs.get(source).is_some_and(|rows| rows.is_empty()) {
            continue;
        }
        return false;
    }
    true
}

fn find_close(stmt: &[Token], open_idx: usize) -> Option<usize> {
    let group = stmt[open_idx].group;
    stmt.iter()
        .enumerate()
        .skip(open_idx + 1)
        .find_map(|(j, t)| {
            if t.kind == TokenKind::Brace && t.value == ")" && t.group == group {
                Some(j)
            } else {
                None
            }
        })
}

fn compile_order_by(
    stmt: &[Token],
    order_idx: usize,
    end: usize,
    ctx: &Ctx<'_>,
) -> Result<Option<(bool, Option<Vec<String>>, usize)>> {
    let mut j = order_idx + 1;
    while j < end && stmt[j].is_ws() {
        j += 1;
    }
    if j >= end || !stmt[j].word_eq("by") {
        return Ok(Some((false, None, order_idx)));
    }
    let mut k = j + 1;
    while k < end && stmt[k].is_ws() {
        k += 1;
    }
    let (terms, end_idx, trailing) = split_order_terms(&stmt[..end], k);
    let mut rendered = Vec::new();
    for term in terms {
        let sort = term.iter().find(|t| t.kind == TokenKind::Sort);
        if sort.is_none() {
            let text: String = term.iter().map(|t| t.value.as_str()).collect();
            rendered.push(text.trim().to_string());
            continue;
        }
        let param = &sort.unwrap().sort_param;
        if let Some(expr) = ctx.sort_map.get(param).and_then(|v| v.as_ref()) {
            rendered.push(expr.clone());
        }
    }
    if rendered.is_empty() {
        return Ok(Some((true, None, end_idx)));
    }
    let mut fragments: Vec<String> = stmt[order_idx..k].iter().map(|t| t.value.clone()).collect();
    fragments.push(rendered.join(", "));
    fragments.push(trailing);
    Ok(Some((true, Some(fragments), end_idx)))
}

fn is_ws_fragment(value: &str) -> bool {
    value.is_empty() || value.chars().all(|c| c.is_whitespace())
}

fn strip_connector(tokens: &mut Vec<String>) {
    let mut i = tokens.len();
    while i > 0 && is_ws_fragment(&tokens[i - 1]) {
        i -= 1;
    }
    if i == 0 {
        return;
    }
    let word = tokens[i - 1].trim().to_ascii_lowercase();
    if word != "and" && word != "or" {
        return;
    }
    tokens.truncate(i - 1);
    while tokens.last().is_some_and(|t| is_ws_fragment(t)) {
        tokens.pop();
    }
}

const CLAUSE_BOUNDARY: &[&str] = &[
    "order",
    "group",
    "having",
    "limit",
    "offset",
    "fetch",
    "for",
    "union",
    "except",
    "intersect",
    ")",
    "where",
    "prewhere",
];
const FILTERS: &[&str] = &["where", "prewhere", "having"];

fn cleanup(mut tokens: Vec<String>) -> Vec<String> {
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 0;
        while i < tokens.len() {
            if is_ws_fragment(&tokens[i]) {
                i += 1;
                continue;
            }
            let word = tokens[i].trim().to_ascii_lowercase();
            if !FILTERS.contains(&word.as_str()) {
                i += 1;
                continue;
            }
            let (j, next) = next_significant(&tokens, i + 1);
            if next.as_deref() == Some("and") || next.as_deref() == Some("or") {
                if let Some(j) = j {
                    let (k, after) = next_significant(&tokens, j + 1);
                    if k.is_some()
                        && after
                            .as_ref()
                            .is_some_and(|w| !CLAUSE_BOUNDARY.contains(&w.as_str()))
                    {
                        let k = k.unwrap();
                        tokens.drain(j..k);
                        changed = true;
                        break;
                    }
                }
            }
            if j.is_none()
                || next
                    .as_ref()
                    .is_some_and(|w| CLAUSE_BOUNDARY.contains(&w.as_str()))
            {
                let old_i = i;
                i = trim_ws_before(&mut tokens, i);
                if j.is_none() {
                    tokens.truncate(i);
                } else {
                    let j = j.unwrap() - (old_i - i);
                    tokens.drain(i..j);
                    if i > 0
                        && i < tokens.len()
                        && !is_ws_fragment(&tokens[i - 1])
                        && !is_ws_fragment(&tokens[i])
                        && tokens[i].trim() != ")"
                    {
                        tokens.insert(i, " ".to_string());
                    }
                }
                changed = true;
                break;
            }
            let j = j.unwrap();
            if let Some(one_end) = match_one_equals_one(&tokens, j) {
                let (k, next_word) = next_significant(&tokens, one_end);
                if k.is_none()
                    || next_word
                        .as_ref()
                        .is_some_and(|w| CLAUSE_BOUNDARY.contains(&w.as_str()))
                {
                    let old_i = i;
                    i = trim_ws_before(&mut tokens, i);
                    let one_end = one_end - (old_i - i);
                    tokens.drain(i..one_end);
                    while i < tokens.len() && is_ws_fragment(&tokens[i]) {
                        if k.is_some() {
                            break;
                        }
                        tokens.remove(i);
                    }
                    if k.is_none() {
                        while tokens.last().is_some_and(|t| is_ws_fragment(t)) {
                            tokens.pop();
                        }
                    }
                    changed = true;
                    break;
                }
                if next_word.as_deref() == Some("and") || next_word.as_deref() == Some("or") {
                    let k = k.unwrap();
                    let mut del_end = k + 1;
                    while del_end < tokens.len() && is_ws_fragment(&tokens[del_end]) {
                        del_end += 1;
                    }
                    tokens.drain(j..del_end);
                    changed = true;
                    break;
                }
            }
            i += 1;
        }
    }
    tokens
}

fn next_significant(tokens: &[String], mut i: usize) -> (Option<usize>, Option<String>) {
    while i < tokens.len() {
        if !is_ws_fragment(&tokens[i]) {
            return (Some(i), Some(tokens[i].trim().to_ascii_lowercase()));
        }
        i += 1;
    }
    (None, None)
}

fn match_one_equals_one(tokens: &[String], i: usize) -> Option<usize> {
    let mut parts = Vec::new();
    let mut j = i;
    while j < tokens.len() && parts.len() < 3 {
        if is_ws_fragment(&tokens[j]) {
            j += 1;
            continue;
        }
        parts.push((j, tokens[j].trim().to_string()));
        j += 1;
        if parts.len() == 1 && is_one_equals_one_compact(&parts[0].1) {
            return Some(parts[0].0 + 1);
        }
    }
    if parts.len() >= 3 && parts[0].1 == "1" && parts[1].1 == "=" && parts[2].1 == "1" {
        return Some(parts[2].0 + 1);
    }
    None
}

fn is_one_equals_one_compact(text: &str) -> bool {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    compact == "1=1"
}

fn trim_ws_before(tokens: &mut Vec<String>, mut i: usize) -> usize {
    while i > 0 && is_ws_fragment(&tokens[i - 1]) {
        tokens.remove(i - 1);
        i -= 1;
    }
    i
}
