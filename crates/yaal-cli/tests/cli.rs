use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};

fn yaal(args: &[&str], stdin: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_yaal"));
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }
    let mut child = cmd.spawn().unwrap();
    if let Some(text) = stdin {
        let mut pipe = child.stdin.take().unwrap();
        match pipe.write_all(text.as_bytes()) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(err) => panic!("write stdin: {err}"),
        }
    }
    child.wait_with_output().unwrap()
}

fn compile(args: &[&str], stdin: Option<&str>) -> Value {
    let output = yaal(args, stdin);
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn version_prints_package_version() {
    for flag in ["--version", "-V"] {
        let output = yaal(&[flag], None);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            format!("yaal {}", env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn help_exits_successfully() {
    for args in [
        &["--help"][..],
        &["-h"],
        &["help"],
        &["compile", "--help"],
        &["compile", "-h"],
    ] {
        let output = yaal(args, None);
        assert!(output.status.success(), "{args:?}");
        assert!(String::from_utf8(output.stdout)
            .unwrap()
            .contains("yaal compile"));
    }
}

#[test]
fn missing_or_unknown_command_fails() {
    let missing = yaal(&[], None);
    assert!(!missing.status.success());
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8(missing.stderr)
        .unwrap()
        .contains("yaal compile"));

    let unknown = yaal(&["fmt"], None);
    assert!(!unknown.status.success());
    let err = String::from_utf8(unknown.stderr).unwrap();
    assert!(err.contains("unknown command fmt"));
    assert!(err.contains("yaal compile"));
}

#[test]
fn compile_param_and_stdin() {
    let sql = "--(id, active)--\nselect * from users u where optional(u.id = {{id}}) and optional(u.active = {{active}})\n";
    let dir = std::env::temp_dir();
    let sql_path = dir.join(format!("yaal-cli-query-{}.sql", std::process::id()));
    let params_path = dir.join(format!("yaal-cli-params-{}.json", std::process::id()));
    std::fs::write(&sql_path, sql).unwrap();
    std::fs::write(&params_path, r#"{"active": false}"#).unwrap();

    let from_file = compile(
        &[
            "compile",
            "--sql",
            sql_path.to_str().unwrap(),
            "--params",
            params_path.to_str().unwrap(),
            "--param",
            "id=1",
        ],
        None,
    );
    let from_stdin = compile(
        &["compile", "--param", "id=1", "--param", "active=false"],
        Some(sql),
    );
    assert_eq!(from_file, from_stdin);
    assert_eq!(
        from_file,
        json!({
            "statements": [{
                "sql": "\nselect * from users u where (u.id = ?) and (u.active = ?)\n",
                "parameters": [
                    {"name": "id", "type": "integer", "value": 1},
                    {"name": "active", "type": "bool", "value": false}
                ]
            }]
        })
    );

    let numbered = compile(
        &[
            "compile",
            "--placeholder",
            "numbered",
            "--param",
            "id=[1, 2]",
        ],
        Some("--(id)--\nwhere optional(u.user_id in ({{id}}...))\n"),
    );
    assert_eq!(
        numbered["statements"][0]["sql"],
        json!("\nwhere (u.user_id in ($1, $2))\n")
    );
    assert_eq!(
        numbered["statements"][0]["parameters"],
        json!([
            {"name": "id", "type": "integer", "value": 1},
            {"name": "id", "type": "integer", "value": 2}
        ])
    );

    let _ = std::fs::remove_file(sql_path);
    let _ = std::fs::remove_file(params_path);
}

#[test]
fn bad_placeholder_fails() {
    let output = yaal(
        &["compile", "--placeholder", "dollar", "--param", "id=1"],
        Some("--(id)--\nselect {{id}}\n"),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("unknown placeholder"));
}
