mod util;

use std::process::Command;

#[test]
fn schema_csv() {
    let path = util::fixtures_dir().join("sample.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args(["schema", path.to_str().unwrap()])
        .output()
        .expect("run megrez schema");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = "format: CSV\nname\ttype\tnullable\nid\tint\tfalse\nname\tstring\ttrue\nscore\tfloat\ttrue\nactive\tbool\ttrue\n";
    assert_eq!(stdout, expected);
}

#[test]
fn schema_json() {
    let path = util::fixtures_dir().join("sample.json");
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args(["schema", path.to_str().unwrap()])
        .output()
        .expect("run megrez schema");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = "format: JSON\nname\ttype\tnullable\nid\tint\tfalse\ntags\tlist<string>\tfalse\nuser.active\tbool\ttrue\nuser.id\tstring\tfalse\n";
    assert_eq!(stdout, expected);
}

#[test]
fn schema_json_hide_format_and_columns() {
    let path = util::fixtures_dir().join("sample.json");
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args([
            "schema",
            path.to_str().unwrap(),
            "--show-format-name=false",
            "--show-columns=false",
        ])
        .output()
        .expect("run megrez schema");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = "id\tint\tfalse\ntags\tlist<string>\tfalse\nuser.active\tbool\ttrue\nuser.id\tstring\tfalse\n";
    assert_eq!(stdout, expected);
}

#[test]
fn schema_mislabeled_parquet_detects_magic() {
    let path = util::ensure_mislabeled_parquet_fixture().expect("create dummy fixture");
    let output = Command::new(env!("CARGO_BIN_EXE_megrez"))
        .args(["schema", path.to_str().unwrap()])
        .output()
        .expect("run megrez schema");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = "format: PARQUET\nname\ttype\tnullable\nid\tint\tfalse\nname\tstring\ttrue\nactive\tbool\tfalse\n";
    assert_eq!(stdout, expected);
}
