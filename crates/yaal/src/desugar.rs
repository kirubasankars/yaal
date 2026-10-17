// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{err, Result};
use crate::header::{Decl, METHOD};
use crate::lex::braced;
use crate::token::{Token, TokenKind};

pub struct Stmt {
    pub content: Vec<Token>,
    pub nullable: BTreeSet<String>,
}

pub struct Program {
    pub decls: BTreeMap<String, Decl>,
    pub stmts: Vec<Stmt>,
    pub expand_names: BTreeSet<String>,
}

pub fn parse_program(mut tokens: Vec<Token>) -> Result<Program> {
    tokens = desugar_optional_when(tokens, false)?;
    tokens = desugar_optional(tokens, false)?;
    let decls_early = scan_header(&tokens)?;
    tokens = desugar_groups(tokens, &decls_early)?;
    tokens = desugar_sort_dir(tokens)?;
    split_statements(tokens)
}

fn scan_header(tokens: &[Token]) -> Result<BTreeMap<String, Decl>> {
    for token in tokens {
        if token.is_ws() {
            continue;
        }
        if token.kind != TokenKind::Dash {
            return Ok(BTreeMap::new());
        }
        match crate::header::parse_header(&token.value) {
            Ok(Some(decls)) => {
                return Ok(decls.into_iter().map(|d| (d.name.clone(), d)).collect());
            }
            Ok(None) => return Ok(BTreeMap::new()),
            Err(_) => return Ok(BTreeMap::new()),
        }
    }
    Ok(BTreeMap::new())
}

fn skip_ws(tokens: &[Token], mut i: usize) -> usize {
    while i < tokens.len() && tokens[i].is_ws() {
        i += 1;
    }
    i
}

fn matching_close(tokens: &[Token], open_index: usize) -> Option<usize> {
    let group = tokens[open_index].group;
    for (k, t) in tokens.iter().enumerate().skip(open_index + 1) {
        if t.kind == TokenKind::Brace && t.value == ")" && t.group == group {
            return Some(k);
        }
    }
    None
}

fn group_join(token: &Token) -> Result<Option<bool>> {
    if token.kind != TokenKind::Word {
        return Ok(None);
    }
    match token.value.to_ascii_lowercase().as_str() {
        "optional_groups" => Err(err(
            "optional_groups(...) was renamed: use optional_groups_or(...) to keep the OR join, or optional_groups_and(...) to AND the rows",
        )),
        "optional_groups_or" => Ok(Some(false)),
        "optional_groups_and" => Ok(Some(true)),
        _ => Ok(None),
    }
}

fn optional_body_params(body: &[Token]) -> Result<(Vec<String>, bool, bool)> {
    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    let mut saw_groups = false;
    let mut saw_self = false;
    let mut i = 0;
    while i < body.len() {
        if group_join(&body[i])?.is_some() {
            saw_groups = true;
            let j = skip_ws(body, i + 1);
            if j < body.len() && body[j].kind == TokenKind::Brace && body[j].value == "(" {
                if let Some(k) = matching_close(body, j) {
                    i = k + 1;
                    continue;
                }
            }
        }
        if body[i].kind == TokenKind::Parameter {
            if body[i].is_self {
                saw_self = true;
            } else if seen.insert(body[i].name.clone()) {
                names.push(body[i].name.clone());
            }
        }
        i += 1;
    }
    Ok((names, saw_groups, saw_self))
}

fn desugar_optional(tokens: Vec<Token>, in_group: bool) -> Result<Vec<Token>> {
    if tokens.is_empty() {
        return Ok(tokens);
    }
    let mut result = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let is_optional =
            tokens[i].kind == TokenKind::Word && tokens[i].value.eq_ignore_ascii_case("optional");
        if is_optional {
            let j = skip_ws(&tokens, i + 1);
            if j < tokens.len() && tokens[j].kind == TokenKind::Brace && tokens[j].value == "(" {
                let k = matching_close(&tokens, j).ok_or_else(|| err("unclosed optional(...)"))?;
                let body = desugar_optional(tokens[j + 1..k].to_vec(), in_group)?;
                let (param_names, saw_groups, saw_self) = optional_body_params(&body)?;
                if param_names.is_empty() && !saw_groups {
                    if in_group || saw_self {
                        return Err(err(
                            "optional(...) inside optional_groups_*(...) must use at least one {{param}}; blob row fields are always required",
                        ));
                    }
                    return Err(err(
                        "optional(...) requires at least one {{param}} in its body",
                    ));
                }
                let mut open = tokens[j].clone();
                if !param_names.is_empty() {
                    open.nullable_parameters = param_names;
                }
                if saw_groups && open.nullable_parameters.is_empty() {
                    open.groups_wrapper = true;
                }
                result.push(open);
                result.extend(body);
                result.push(tokens[k].clone());
                i = k + 1;
                continue;
            }
        }
        result.push(tokens[i].clone());
        i += 1;
    }
    Ok(result)
}

fn desugar_optional_when(tokens: Vec<Token>, in_group: bool) -> Result<Vec<Token>> {
    if tokens.is_empty() {
        return Ok(tokens);
    }
    let mut result = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let is_when = tokens[i].kind == TokenKind::Word
            && tokens[i].value.eq_ignore_ascii_case("optional_when");
        if is_when {
            let j = skip_ws(&tokens, i + 1);
            if j < tokens.len() && tokens[j].kind == TokenKind::Brace && tokens[j].value == "(" {
                let k =
                    matching_close(&tokens, j).ok_or_else(|| err("unclosed optional_when(...)"))?;
                let inner = &tokens[j + 1..k];
                let mut p = skip_ws(inner, 0);
                if p >= inner.len() || inner[p].kind != TokenKind::Parameter {
                    return Err(err(
                        "optional_when(...) requires {{param}} as the first argument",
                    ));
                }
                if inner[p].is_self {
                    return Err(err(
                        "optional_when(...) inside optional_groups_*(...) must use a {{param}} condition; blob row fields are always required",
                    ));
                }
                if inner[p].expand {
                    return Err(err(format!(
                        "{}... cannot be an optional_when condition",
                        braced(&inner[p].name)
                    )));
                }
                let condition = inner[p].name.clone();
                p = skip_ws(inner, p + 1);
                if p >= inner.len() || inner[p].kind != TokenKind::Word || inner[p].value != "," {
                    return Err(err(
                        "optional_when(...) requires a comma after the condition parameter",
                    ));
                }
                let body_raw = inner[skip_ws(inner, p + 1)..].to_vec();
                ensure_no_nested_when(&body_raw)?;
                let body = desugar_optional(desugar_optional_when(body_raw, in_group)?, in_group)?;
                let (param_names, saw_groups, saw_self) = optional_body_params(&body)?;
                if param_names.is_empty() && !saw_groups && !saw_self {
                    return Err(err(
                        "optional_when(...) requires at least one {{param}} in its body",
                    ));
                }
                let mut open = tokens[j].clone();
                open.optional_when = Some(condition);
                if !param_names.is_empty() {
                    open.nullable_parameters = param_names;
                }
                if saw_groups && open.nullable_parameters.is_empty() {
                    open.groups_wrapper = true;
                }
                result.push(open);
                result.extend(body);
                result.push(tokens[k].clone());
                i = k + 1;
                continue;
            }
        }
        result.push(tokens[i].clone());
        i += 1;
    }
    Ok(result)
}

fn ensure_no_nested_when(body: &[Token]) -> Result<()> {
    for t in body {
        if t.kind == TokenKind::Word && t.value.eq_ignore_ascii_case("optional_when") {
            return Err(err("nested optional_when(...) is not supported"));
        }
    }
    Ok(())
}

fn ensure_no_nested_groups(body: &[Token]) -> Result<()> {
    for t in body {
        if group_join(t)?.is_some() {
            return Err(err(
                "nested optional_groups_or(...)/optional_groups_and(...) is not supported",
            ));
        }
    }
    Ok(())
}

fn desugar_groups(tokens: Vec<Token>, decls: &BTreeMap<String, Decl>) -> Result<Vec<Token>> {
    if tokens.is_empty() {
        return Ok(tokens);
    }
    let mut result = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if let Some(join_and) = group_join(&tokens[i])? {
            let keyword = tokens[i].value.to_ascii_lowercase();
            let j = skip_ws(&tokens, i + 1);
            if j < tokens.len() && tokens[j].kind == TokenKind::Brace && tokens[j].value == "(" {
                let k = matching_close(&tokens, j)
                    .ok_or_else(|| err(format!("unclosed {keyword}(...)")))?;
                let inner = &tokens[j + 1..k];
                let mut p = skip_ws(inner, 0);
                if p >= inner.len() || inner[p].kind != TokenKind::Parameter {
                    return Err(err(format!(
                        "{keyword}(...) requires {{{{blob}}}} as the first argument"
                    )));
                }
                if inner[p].is_self {
                    return Err(err(format!(
                        "{keyword}(...) requires {{{{blob}}}} as the first argument"
                    )));
                }
                if inner[p].expand {
                    return Err(err(format!(
                        "{}... cannot be an optional_groups_* blob source",
                        braced(&inner[p].name)
                    )));
                }
                let source_name = inner[p].name.clone();
                p = skip_ws(inner, p + 1);
                if p >= inner.len() || inner[p].kind != TokenKind::Word || inner[p].value != "," {
                    return Err(err(format!(
                        "{keyword}(...) requires a comma after the blob parameter"
                    )));
                }
                let body_raw = inner[skip_ws(inner, p + 1)..].to_vec();
                ensure_no_nested_groups(&body_raw)?;
                let body = desugar_groups(body_raw, decls)?;
                let body = desugar_optional(body, true)?;
                let (body, field_order) = mark_group_fields(body, &source_name)?;
                if field_order.is_empty() {
                    return Err(err(format!(
                        "{keyword}(...) requires at least one {{{{self.field}}}} in its body"
                    )));
                }
                let _ = decls;
                let mut open = tokens[j].clone();
                open.group_source = Some(source_name);
                open.group_fields = field_order;
                open.group_join_and = join_and;
                result.push(open);
                result.extend(body);
                result.push(tokens[k].clone());
                i = k + 1;
                continue;
            }
        }
        result.push(tokens[i].clone());
        i += 1;
    }
    Ok(result)
}

fn mark_group_fields(body: Vec<Token>, source: &str) -> Result<(Vec<Token>, Vec<String>)> {
    let mut out = Vec::new();
    let mut field_order = Vec::new();
    let mut seen = BTreeSet::new();
    let mut expand_of: BTreeMap<String, bool> = BTreeMap::new();
    let mut row_names = BTreeSet::new();
    for t in body {
        if t.kind != TokenKind::Parameter {
            out.push(t);
            continue;
        }
        if t.is_self {
            if t.name == source {
                return Err(err(format!(
                    "optional_groups_*(...) blob {} must not be used in its body",
                    braced(&format!("self.{}", t.name))
                )));
            }
            if let Some(prev) = expand_of.get(&t.name) {
                if *prev != t.expand {
                    return Err(err(format!(
                        "{} is used both as a single value and as {}...",
                        braced(&format!("self.{}", t.name)),
                        braced(&format!("self.{}", t.name))
                    )));
                }
            } else {
                expand_of.insert(t.name.clone(), t.expand);
            }
            row_names.insert(format!("self.{}", t.name));
            if seen.insert(t.name.clone()) {
                field_order.push(t.name.clone());
            }
            let mut gf = t.clone();
            gf.kind = TokenKind::GroupField;
            out.push(gf);
            continue;
        }
        if t.name == source {
            return Err(err(format!(
                "optional_groups_*(...) blob {} must not be used in its body",
                braced(&t.name)
            )));
        }
        out.push(t);
    }

    for t in &mut out {
        if t.kind != TokenKind::Brace {
            continue;
        }
        if t.optional_when.is_some() && t.nullable_parameters.is_empty() {
            // A condition gates the block, so a row-field-only body is allowed.
        }
        if t.nullable_parameters.is_empty() {
            continue;
        }
        t.nullable_parameters
            .retain(|p| !row_names.contains(p) && !p.starts_with("self."));
        if t.nullable_parameters.is_empty() && t.optional_when.is_none() {
            return Err(err(
                "optional(...) inside optional_groups_*(...) must use at least one {{param}}; blob row fields are always required",
            ));
        }
    }
    Ok((out, field_order))
}

fn body_has_call(tokens: &[Token], names: &[&str]) -> bool {
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].kind == TokenKind::Word
            && names
                .iter()
                .any(|n| tokens[i].value.eq_ignore_ascii_case(n))
        {
            let j = skip_ws(tokens, i + 1);
            if j < tokens.len() && tokens[j].kind == TokenKind::Brace && tokens[j].value == "(" {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn desugar_sort_dir(tokens: Vec<Token>) -> Result<Vec<Token>> {
    if tokens.is_empty() {
        return Ok(tokens);
    }
    let mut result = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let kind = if tokens[i].kind == TokenKind::Word {
            match tokens[i].value.to_ascii_lowercase().as_str() {
                "sort" => Some("sort"),
                "dir" => Some("dir"),
                _ => None,
            }
        } else {
            None
        };
        if let Some(kind) = kind {
            let j = skip_ws(&tokens, i + 1);
            if j < tokens.len() && tokens[j].kind == TokenKind::Brace && tokens[j].value == "(" {
                let k = matching_close(&tokens, j)
                    .ok_or_else(|| err(format!("unclosed {kind}(...)")))?;
                let body = &tokens[j + 1..k];
                if body.is_empty() || body.iter().all(|t| t.is_ws()) {
                    return Err(err(format!("{kind}() empty / no param")));
                }
                if kind == "sort" {
                    let (param, choices) = parse_sort_body(body)?;
                    let mut tok = Token::new(TokenKind::Sort, "");
                    tok.sort_param = param;
                    tok.name = tok.sort_param.clone();
                    tok.sort_choices = choices;
                    result.push(tok);
                } else {
                    let param = parse_dir_body(body)?;
                    let mut tok = Token::new(TokenKind::Dir, "");
                    tok.sort_param = param;
                    tok.name = tok.sort_param.clone();
                    result.push(tok);
                }
                i = k + 1;
                continue;
            }
        }
        result.push(tokens[i].clone());
        i += 1;
    }
    Ok(result)
}

fn parse_sort_body(body: &[Token]) -> Result<(String, BTreeMap<String, String>)> {
    if body_has_call(body, &["sort", "dir"]) {
        return Err(err("nested sort(...)/dir(...) is not allowed"));
    }
    let mut i = skip_ws(body, 0);
    if i >= body.len() || body[i].kind != TokenKind::Parameter {
        return Err(err("sort(...) requires {{param}} as the first argument"));
    }
    if body[i].is_self {
        return Err(err("sort(...) requires {{param}} as the first argument"));
    }
    if body[i].expand {
        return Err(err(format!(
            "{}... is not allowed on sort()",
            braced(&body[i].name)
        )));
    }
    let param_name = body[i].name.clone();
    let param_count = body
        .iter()
        .filter(|t| t.kind == TokenKind::Parameter)
        .count();
    if param_count != 1 {
        return Err(err("sort(...) requires exactly one {{param}}"));
    }
    i = skip_ws(body, i + 1);
    if i >= body.len() {
        return Err(err("sort(...) requires at least one key = expr pair"));
    }
    if body[i].kind == TokenKind::Word && body[i].value == "," {
        i = skip_ws(body, i + 1);
    } else if body[i].kind == TokenKind::Word && body[i].value.starts_with(',') {
        return Err(err("invalid sort(...) argument list"));
    }
    if i >= body.len() {
        return Err(err("sort(...) requires at least one key = expr pair"));
    }
    let mut choices = BTreeMap::new();
    while i < body.len() {
        if body[i].kind != TokenKind::Word || !is_sort_key(&body[i].value) {
            return Err(err(format!(
                "sort(...) keys must be word characters (\\w+), got {:?}",
                body[i].value
            )));
        }
        let key = body[i].value.to_ascii_lowercase();
        if choices.contains_key(&key) {
            return Err(err(format!("duplicate sort(...) key: {key}")));
        }
        i = skip_ws(body, i + 1);
        if i >= body.len() || body[i].kind != TokenKind::Word || body[i].value != "=" {
            return Err(err("sort(...) expected key = expr"));
        }
        i = skip_ws(body, i + 1);
        if i >= body.len() {
            return Err(err("sort(...) expected expression after ="));
        }
        let mut expr_tokens = Vec::new();
        let mut depth = 0i32;
        while i < body.len() {
            let t = &body[i];
            if t.kind == TokenKind::Brace {
                if t.value == "(" {
                    depth += 1;
                } else if t.value == ")" {
                    depth -= 1;
                }
                expr_tokens.push(t.clone());
                i += 1;
                continue;
            }
            if depth == 0 && t.kind == TokenKind::Word {
                if t.value == "," {
                    i += 1;
                    break;
                }
                if t.value.ends_with(',') && !t.value[..t.value.len() - 1].contains(',') {
                    let mut trimmed = t.clone();
                    trimmed.value = t.value[..t.value.len() - 1].to_string();
                    if !trimmed.value.is_empty() {
                        expr_tokens.push(trimmed);
                    }
                    i += 1;
                    break;
                }
            }
            expr_tokens.push(t.clone());
            i += 1;
        }
        while expr_tokens.last().is_some_and(|t| t.is_ws()) {
            expr_tokens.pop();
        }
        while expr_tokens.first().is_some_and(|t| t.is_ws()) {
            expr_tokens.remove(0);
        }
        let expr_sql: String = expr_tokens.iter().map(|t| t.value.as_str()).collect();
        let expr_sql = expr_sql.trim().to_string();
        if expr_sql.is_empty() {
            return Err(err(format!("sort(...) empty expression for key {key}")));
        }
        choices.insert(key, expr_sql);
        i = skip_ws(body, i);
    }
    if choices.is_empty() {
        return Err(err("sort(...) requires at least one key = expr pair"));
    }
    Ok((param_name, choices))
}

fn is_sort_key(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn parse_dir_body(body: &[Token]) -> Result<String> {
    if body_has_call(body, &["sort", "dir"]) {
        return Err(err("nested sort(...)/dir(...) is not allowed"));
    }
    let i = skip_ws(body, 0);
    if i >= body.len() || body[i].kind != TokenKind::Parameter {
        return Err(err("dir(...) requires exactly one {{param}}"));
    }
    if body[i].is_self || body[i].expand {
        return Err(err(format!(
            "{}... is not allowed on dir()",
            braced(&body[i].name)
        )));
    }
    let param_name = body[i].name.clone();
    let param_count = body
        .iter()
        .filter(|t| t.kind == TokenKind::Parameter)
        .count();
    if param_count != 1 {
        return Err(err("dir(...) requires exactly one {{param}}"));
    }
    let next = skip_ws(body, i + 1);
    if next < body.len() {
        return Err(err("dir(...) does not accept key = expr pairs"));
    }
    Ok(param_name)
}

fn has_significant(content: &[Token]) -> bool {
    content
        .iter()
        .any(|t| t.kind == TokenKind::Word && !t.value.trim().is_empty())
}

fn split_statements(tokens: Vec<Token>) -> Result<Program> {
    let mut decls: BTreeMap<String, Decl> = BTreeMap::new();
    let mut stmts = Vec::new();
    let mut content = Vec::new();
    let mut significant = false;
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        if token.is_ws() && !significant {
            i += 1;
            continue;
        }
        if token.kind == TokenKind::Dash {
            if !significant {
                if let Some(parsed) = crate::header::parse_header(&token.value)? {
                    decls = parsed.into_iter().map(|d| (d.name.clone(), d)).collect();
                    significant = true;
                    i += 1;
                    continue;
                }
            }
            if has_significant(&content) {
                stmts.push(std::mem::take(&mut content));
            } else {
                content.clear();
            }
            significant = true;
            i += 1;
            continue;
        }
        content.push(token.clone());
        significant = true;
        i += 1;
    }
    if has_significant(&content) {
        stmts.push(content);
    }

    let mut programs = Vec::new();
    let mut expand_names = BTreeSet::new();
    let mut scalar_names = BTreeSet::new();
    for content in stmts {
        let mut stmt = finish_stmt(content, &decls)?;
        collect_shapes(&stmt, &mut expand_names, &mut scalar_names)?;
        validate_order_by(&stmt)?;
        validate_groups(&stmt, &decls)?;
        // nullable names
        let mut nullable = BTreeSet::new();
        for token in &mut stmt {
            if token.kind == TokenKind::Parameter && token.nullable_marker {
                nullable.insert(token.name.clone());
            }
            if token.kind != TokenKind::Brace {
                continue;
            }
            if let Some(cond) = &token.optional_when {
                nullable.insert(cond.clone());
            }
            for name in &token.nullable_parameters {
                nullable.insert(name.clone());
            }
            if let Some(source) = &token.group_source {
                nullable.insert(source.clone());
            } else if token.optional_when.is_none() && !token.groups_wrapper {
                if let Some(name) = long_form_nullable(token) {
                    token.nullable_parameter = Some(name.clone());
                    nullable.insert(name);
                }
            }
        }
        programs.push(Stmt {
            content: stmt,
            nullable,
        });
    }
    for name in &expand_names {
        if scalar_names.contains(name) {
            return Err(err(format!(
                "{} is used both as a single value and as {}...",
                braced(name),
                braced(name)
            )));
        }
    }
    // Self fields left as parameters are outside a group.
    for stmt in &programs {
        for token in &stmt.content {
            if token.kind == TokenKind::Parameter && token.is_self {
                return Err(err(format!(
                    "{} is only valid inside optional_groups_*(...)",
                    braced(&format!("self.{}", token.name))
                )));
            }
        }
    }
    Ok(Program {
        decls,
        stmts: programs,
        expand_names,
    })
}

fn finish_stmt(content: Vec<Token>, decls: &BTreeMap<String, Decl>) -> Result<Vec<Token>> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut brace_open: BTreeSet<i32> = BTreeSet::new();
    let mut null_markers: Vec<(i32, String)> = Vec::new();
    while i < content.len() {
        let mut token = content[i].clone();
        if token.kind == TokenKind::Sort || token.kind == TokenKind::Dir {
            require_decl(decls, &token.sort_param)?;
        }
        if token.kind == TokenKind::Brace {
            if let Some(cond) = &token.optional_when {
                require_decl(decls, cond)?;
            }
        }
        if token.kind == TokenKind::Parameter && !token.is_self {
            require_decl(decls, &token.name)?;
            if let Some(null_idx) = match_is_null_or(&content, i) {
                if brace_open.is_empty() {
                    return Err(err(format!(
                        "{} is null or must be wrapped in parentheses in {METHOD}.sql",
                        braced(&token.name)
                    )));
                }
                token.nullable_marker = true;
                token.value = format!("{} is null", braced(&token.name));
                if let Some(group) = brace_open.iter().max().copied() {
                    null_markers.push((group, token.name.clone()));
                }
                out.push(token);
                i = null_idx + 1;
                continue;
            }
        }
        if token.kind == TokenKind::Brace {
            if token.value == "(" {
                brace_open.insert(token.group);
            } else if token.value == ")" {
                brace_open.remove(&token.group);
            }
        }
        out.push(token);
        i += 1;
    }
    for (group, name) in null_markers {
        for token in &mut out {
            if token.kind == TokenKind::Brace
                && token.value == "("
                && token.group == group
                && token.nullable_parameter.is_none()
            {
                token.nullable_parameter = Some(name);
                break;
            }
        }
    }
    Ok(out)
}

fn require_decl(decls: &BTreeMap<String, Decl>, name: &str) -> Result<()> {
    if decls.contains_key(name) {
        Ok(())
    } else {
        Err(err(format!(
            "type missing for {} in the {METHOD}.sql",
            braced(name)
        )))
    }
}

fn match_is_null_or(tokens: &[Token], param_index: usize) -> Option<usize> {
    let mut i = skip_ws(tokens, param_index + 1);
    if i >= tokens.len() || !tokens[i].word_eq("is") {
        return None;
    }
    i = skip_ws(tokens, i + 1);
    if i >= tokens.len() || !tokens[i].word_eq("null") {
        return None;
    }
    let null_at = i;
    let j = skip_ws(tokens, i + 1);
    if j >= tokens.len() || !tokens[j].word_eq("or") {
        return None;
    }
    Some(null_at)
}

fn long_form_nullable(token: &Token) -> Option<String> {
    // "( {{name}} is null or"
    let text = token.value.as_str();
    let lower = text.to_ascii_lowercase();
    let start = lower.find("{{")?;
    let end = lower[start..].find("}}")? + start;
    let name = lower[start + 2..end].trim().to_string();
    let after = lower[end + 2..].trim();
    if after.starts_with("is null or") || after.starts_with("is null") {
        // The marker token itself is "{{name}} is null" after rewrite, stored on the
        // parameter. Brace content is accumulated only in the Python parser. The
        // long form is handled by nullable_marker on the parameter token instead.
        let _ = name;
    }
    None
}

fn collect_shapes(
    content: &[Token],
    expand_names: &mut BTreeSet<String>,
    scalar_names: &mut BTreeSet<String>,
) -> Result<()> {
    for token in content {
        if token.kind == TokenKind::Parameter && !token.is_self && !token.nullable_marker {
            if token.expand {
                expand_names.insert(token.name.clone());
            } else {
                scalar_names.insert(token.name.clone());
            }
        }
        if token.kind == TokenKind::Sort || token.kind == TokenKind::Dir {
            scalar_names.insert(token.sort_param.clone());
        }
        if let Some(cond) = &token.optional_when {
            scalar_names.insert(cond.clone());
        }
    }
    Ok(())
}

const ORDER_END: &[&str] = &[
    "limit",
    "offset",
    "fetch",
    "for",
    "union",
    "except",
    "intersect",
    ")",
    ";",
];

fn is_order_end(token: &Token) -> bool {
    (token.kind == TokenKind::Brace && token.value == ")")
        || (token.kind == TokenKind::Word
            && ORDER_END
                .iter()
                .any(|w| token.value.trim().eq_ignore_ascii_case(w)))
}

pub fn split_order_terms(content: &[Token], start: usize) -> (Vec<Vec<Token>>, usize, String) {
    let mut terms: Vec<Vec<Token>> = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    let mut i = start;
    while i < content.len() {
        let t = &content[i];
        if depth == 0 && is_order_end(t) {
            break;
        }
        if t.kind == TokenKind::Brace {
            if t.value == "(" {
                depth += 1;
            } else if t.value == ")" {
                depth -= 1;
            }
            current.push(t.clone());
            i += 1;
            continue;
        }
        if depth == 0 && t.kind == TokenKind::Word {
            if t.value == "," {
                terms.push(std::mem::take(&mut current));
                i += 1;
                continue;
            }
            if t.value.ends_with(',') && !t.value[..t.value.len() - 1].contains(',') {
                let mut trimmed = t.clone();
                trimmed.value = t.value[..t.value.len() - 1].to_string();
                if !trimmed.value.is_empty() {
                    current.push(trimmed);
                }
                terms.push(std::mem::take(&mut current));
                i += 1;
                continue;
            }
        }
        current.push(t.clone());
        i += 1;
    }
    terms.push(current);
    let trailing = {
        let tail = terms.last_mut().unwrap();
        let mut ws = Vec::new();
        while tail.last().is_some_and(|t| t.is_ws()) {
            ws.insert(0, tail.pop().unwrap());
        }
        ws.into_iter().map(|t| t.value).collect::<String>()
    };
    for term in &mut terms {
        while term.first().is_some_and(|t| t.is_ws()) {
            term.remove(0);
        }
        while term.last().is_some_and(|t| t.is_ws()) {
            term.pop();
        }
    }
    (terms, i, trailing)
}

fn validate_order_by(content: &[Token]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for tok in content {
        if tok.kind == TokenKind::Sort && !seen.insert(tok.sort_param.clone()) {
            return Err(err(format!(
                "{} is used in more than one sort(...) in {METHOD}.sql -- each sort() must use a distinct param",
                braced(&tok.sort_param)
            )));
        }
    }
    let mut i = 0;
    while i < content.len() {
        if content[i].word_eq("order") {
            let j = skip_ws(content, i + 1);
            if j < content.len() && content[j].word_eq("by") {
                let k = skip_ws(content, j + 1);
                let (terms, end_idx, _) = split_order_terms(content, k);
                for term in &terms {
                    if term.is_empty() {
                        return Err(err(format!("empty ORDER BY term in {METHOD}.sql")));
                    }
                }
                let dynamic: Vec<_> = terms
                    .iter()
                    .filter(|term| {
                        term.iter()
                            .any(|t| t.kind == TokenKind::Sort || t.kind == TokenKind::Dir)
                    })
                    .collect();
                if dynamic.len() > 1 {
                    return Err(err(format!(
                        "only one dynamic sort()/dir() term is allowed per ORDER BY in {METHOD}.sql"
                    )));
                }
                if dynamic.len() == 1 {
                    let dyn_term = dynamic[0];
                    let sort_positions: Vec<_> = dyn_term
                        .iter()
                        .enumerate()
                        .filter(|(_, t)| t.kind == TokenKind::Sort)
                        .map(|(i, _)| i)
                        .collect();
                    let dir_positions: Vec<_> = dyn_term
                        .iter()
                        .enumerate()
                        .filter(|(_, t)| t.kind == TokenKind::Dir)
                        .map(|(i, _)| i)
                        .collect();
                    if sort_positions.len() != 1 {
                        return Err(err(format!(
                            "ORDER BY dynamic term must contain exactly one sort(...) in {METHOD}.sql"
                        )));
                    }
                    if dir_positions.len() > 1 {
                        return Err(err(format!(
                            "ORDER BY dynamic term must contain at most one dir(...) in {METHOD}.sql"
                        )));
                    }
                    let non_ws: Vec<_> = dyn_term
                        .iter()
                        .enumerate()
                        .filter(|(_, t)| !t.is_ws())
                        .map(|(i, _)| i)
                        .collect();
                    if !dir_positions.is_empty() {
                        let s_pos = non_ws.iter().position(|i| *i == sort_positions[0]).unwrap();
                        let d_pos = non_ws.iter().position(|i| *i == dir_positions[0]).unwrap();
                        if d_pos != s_pos + 1 || non_ws.len() != 2 {
                            return Err(err(format!(
                                "ORDER BY with sort()/dir() must not include other terms in {METHOD}.sql"
                            )));
                        }
                    } else if non_ws.len() != 1 {
                        return Err(err(format!(
                            "ORDER BY with sort()/dir() must not include other terms in {METHOD}.sql"
                        )));
                    }
                }
                i = end_idx;
                continue;
            }
        }
        i += 1;
    }
    Ok(())
}

fn validate_groups(content: &[Token], decls: &BTreeMap<String, Decl>) -> Result<()> {
    for tok in content {
        let Some(source) = &tok.group_source else {
            continue;
        };
        let decl = decls.get(source).ok_or_else(|| {
            err(format!(
                "type missing for {} in the {METHOD}.sql",
                braced(source)
            ))
        })?;
        if !decl.blob {
            return Err(err(format!(
                "optional_groups_*(...) source {} must be declared blob in {METHOD}.sql",
                braced(source)
            )));
        }
    }
    Ok(())
}
