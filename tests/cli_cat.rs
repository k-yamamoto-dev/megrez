mod util;

use anyhow::Result;
use serde_json::Value;
use std::process::Command;

#[test]
fn cat_parquet_limit() -> Result<()> {
    let path = util::ensure_parquet_fixture()?;
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args(["cat", path.to_str().unwrap(), "--limit", "2"])
        .output()
        .expect("run megrez cat");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("parse json"))
        .collect();

    let expected = vec![
        serde_json::json!({"id": 1, "name": "alice", "active": true}),
        serde_json::json!({"id": 2, "name": "bob", "active": false}),
    ];
    assert_eq!(lines, expected);
    Ok(())
}

#[test]
fn cat_avro_limit() -> Result<()> {
    let path = util::ensure_avro_fixture()?;
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args(["cat", path.to_str().unwrap(), "--limit", "2"])
        .output()
        .expect("run megrez cat");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("parse json"))
        .collect();

    let expected = vec![
        serde_json::json!({"id": 1, "name": "alice", "active": true}),
        serde_json::json!({"id": 2, "name": null, "active": false}),
    ];
    assert_eq!(lines, expected);
    Ok(())
}
