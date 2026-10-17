// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

use crate::error::{err, Result};
use crate::token::{is_ident, Token, TokenKind};

pub fn lex(content: &str) -> Result<Vec<Token>> {
    if content.is_empty() {
        return Ok(Vec::new());
    }
    let chars: Vec<char> = content.chars().collect();
    let n = chars.len();
    let mut tokens = Vec::new();
    let mut current = 0;
    let mut brace_group = 0i32;

    while current < n {
        let p = chars[current];
        let p1 = if current + 1 < n {
            chars[current + 1]
        } else {
            '\0'
        };

        if p == '\'' || p == '"' {
            let (next, tok) = lex_string(&chars, current, p)?;
            current = next;
            tokens.push(tok);
        } else if p == '-' && p1 == '-' {
            let (next, tok) = lex_dash(&chars, current)?;
            current = next;
            if let Some(tok) = tok {
                tokens.push(tok);
            }
        } else if p == '{' && p1 == '{' {
            let (next, tok) = lex_param(&chars, current)?;
            current = next;
            tokens.push(tok);
        } else if p == ' ' || p == '\t' || p == '\r' {
            let (next, tok) = lex_spaces(&chars, current);
            current = next;
            tokens.push(tok);
        } else if p == '(' || p == ')' {
            let mut tok = Token::new(TokenKind::Brace, p.to_string());
            if p == '(' {
                brace_group += 1;
                tok.group = brace_group;
            } else {
                tok.group = brace_group;
                brace_group -= 1;
            }
            current += 1;
            tokens.push(tok);
        } else if p == '\n' {
            tokens.push(Token::new(TokenKind::Newline, "\n"));
            current += 1;
        } else {
            let (next, tok) = lex_word(&chars, current);
            current = next;
            tokens.push(tok);
        }
    }
    Ok(tokens)
}

fn lex_dash(chars: &[char], mut current: usize) -> Result<(usize, Option<Token>)> {
    let n = chars.len();
    current += 2;
    if current >= n {
        return Ok((current, None));
    }

    let mut j = current;
    while j < n && matches!(chars[j], ' ' | '\t' | '\r') {
        j += 1;
    }
    if j > current && j < n && chars[j] == '(' {
        return Err(err(
            "invalid parameter header: use --(name type)-- without space after --",
        ));
    }

    if chars[current] == '(' {
        let mut raw = String::from("--(");
        current += 1;
        while current < n {
            if current + 2 < n
                && chars[current] == ')'
                && chars[current + 1] == '-'
                && chars[current + 2] == '-'
            {
                raw.push_str(")--");
                current += 3;
                return Ok((current, Some(Token::new(TokenKind::Dash, raw))));
            }
            raw.push(chars[current]);
            current += 1;
        }
        return Err(err("unclosed parameter header --(...)--"));
    }

    if starts_with(chars, current, "sql") {
        let mut raw = String::from("--");
        while current < n {
            let p = chars[current];
            let p1 = if current + 1 < n {
                chars[current + 1]
            } else {
                '\0'
            };
            if p == '-' && p1 == '-' {
                raw.push_str("--");
                current += 2;
                return Ok((current, Some(Token::new(TokenKind::Dash, raw))));
            }
            raw.push(p);
            current += 1;
        }
        return Err(err("unclosed --sql-- directive"));
    }

    while current < n && chars[current] != '\n' {
        current += 1;
    }
    Ok((current, None))
}

fn lex_param(chars: &[char], mut current: usize) -> Result<(usize, Token)> {
    let n = chars.len();
    current += 2;
    let mut inner = String::new();
    while current < n {
        let p = chars[current];
        let p1 = if current + 1 < n {
            chars[current + 1]
        } else {
            '\0'
        };
        if p == '}' && p1 == '}' {
            current += 2;
            let expand = current + 2 < n
                && chars[current] == '.'
                && chars[current + 1] == '.'
                && chars[current + 2] == '.';
            if expand {
                current += 3;
            }
            let (name, is_self) = classify_param(&inner)?;
            let shown = inner.trim();
            let mut tok = Token::new(TokenKind::Parameter, format!("{{{{{shown}}}}}"));
            tok.name = name;
            tok.is_self = is_self;
            tok.expand = expand;
            return Ok((current, tok));
        }
        inner.push(p);
        current += 1;
    }
    Err(err("unclosed {{...}} parameter"))
}

fn classify_param(inner: &str) -> Result<(String, bool)> {
    let text = inner.trim().to_ascii_lowercase();
    if let Some(field) = text.strip_prefix("self.") {
        if !is_ident(field) {
            return Err(err(format!("invalid row field {}", braced(inner.trim()))));
        }
        return Ok((field.to_string(), true));
    }
    if !is_ident(&text) {
        return Err(err(format!("invalid parameter {}", braced(inner.trim()))));
    }
    Ok((text, false))
}

fn lex_string(chars: &[char], mut current: usize, quote: char) -> Result<(usize, Token)> {
    let n = chars.len();
    let mut raw = String::new();
    raw.push(quote);
    current += 1;
    while current < n {
        let p = chars[current];
        let p1 = if current + 1 < n {
            chars[current + 1]
        } else {
            '\0'
        };
        if p == quote && p1 != quote {
            raw.push(quote);
            current += 1;
            return Ok((current, Token::new(TokenKind::String, raw)));
        }
        raw.push(p);
        current += 1;
    }
    Err(err("unclosed string literal"))
}

fn lex_spaces(chars: &[char], start: usize) -> (usize, Token) {
    let n = chars.len();
    let mut current = start;
    while current < n && matches!(chars[current], ' ' | '\t' | '\r') {
        current += 1;
    }
    let value: String = chars[start..current].iter().collect();
    (current, Token::new(TokenKind::Space, value))
}

fn lex_word(chars: &[char], start: usize) -> (usize, Token) {
    let n = chars.len();
    let mut current = start;
    while current < n {
        let p = chars[current];
        let p1 = if current + 1 < n {
            chars[current + 1]
        } else {
            '\0'
        };
        let single = matches!(p, '(' | ')' | '\'' | '"' | '\n' | ' ' | '\t' | '\r');
        let double = matches!(p, '{' | '}' | '-') && p == p1;
        if single || double {
            break;
        }
        current += 1;
    }
    let value: String = chars[start..current].iter().collect();
    (current, Token::new(TokenKind::Word, value))
}

pub fn braced(name: &str) -> String {
    format!("{{{{{name}}}}}")
}

fn starts_with(chars: &[char], at: usize, text: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    if at + t.len() > chars.len() {
        return false;
    }
    chars[at..at + t.len()] == t[..]
}
