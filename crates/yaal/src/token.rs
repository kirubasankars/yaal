// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    Word,
    String,
    Space,
    Newline,
    Brace,
    Dash,
    Parameter,
    Sort,
    Dir,
    GroupField,
}

#[derive(Clone, Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub value: String,
    pub group: i32,
    pub name: String,
    pub expand: bool,
    pub is_self: bool,
    pub nullable_marker: bool,
    pub nullable_parameters: Vec<String>,
    pub optional_when: Option<String>,
    pub groups_wrapper: bool,
    pub group_source: Option<String>,
    pub group_fields: Vec<String>,
    pub group_join_and: bool,
    pub nullable_parameter: Option<String>,
    pub sort_param: String,
    pub sort_choices: BTreeMap<String, String>,
}

impl Token {
    pub fn new(kind: TokenKind, value: impl Into<String>) -> Self {
        Self {
            kind,
            value: value.into(),
            group: 0,
            name: String::new(),
            expand: false,
            is_self: false,
            nullable_marker: false,
            nullable_parameters: Vec::new(),
            optional_when: None,
            groups_wrapper: false,
            group_source: None,
            group_fields: Vec::new(),
            group_join_and: false,
            nullable_parameter: None,
            sort_param: String::new(),
            sort_choices: BTreeMap::new(),
        }
    }

    pub fn is_ws(&self) -> bool {
        self.kind == TokenKind::Space || self.kind == TokenKind::Newline
    }

    pub fn word_eq(&self, word: &str) -> bool {
        self.kind == TokenKind::Word && self.value.trim().eq_ignore_ascii_case(word)
    }
}

pub fn is_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}
