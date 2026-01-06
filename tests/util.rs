use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

pub fn ensure_parquet_fixture() -> Result<PathBuf> {
    let path = fixtures_dir().join("sample.parquet");
    fs::create_dir_all(fixtures_dir()).context("create fixtures dir")?;
    if path.exists() {
        fs::remove_file(&path).context("remove stale Parquet fixture")?;
    }
    create_parquet(&path)?;
    Ok(path)
}

pub fn ensure_avro_fixture() -> Result<PathBuf> {
    let path = fixtures_dir().join("sample.avro");
    fs::create_dir_all(fixtures_dir()).context("create fixtures dir")?;
    if path.exists() {
        fs::remove_file(&path).context("remove stale Avro fixture")?;
    }
    create_avro(&path)?;
    Ok(path)
}

pub fn ensure_mislabeled_parquet_fixture() -> Result<PathBuf> {
    let path = fixtures_dir().join("dummy-sample.avro");
    fs::create_dir_all(fixtures_dir()).context("create fixtures dir")?;
    if path.exists() {
        fs::remove_file(&path).context("remove stale dummy fixture")?;
    }
    // Intentionally write Parquet bytes to a .avro path to exercise magic-byte detection.
    create_parquet(&path)?;
    Ok(path)
}

fn create_parquet(path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .context("build record batch")?;

    let file = File::create(path).context("create Parquet file")?;
    let mut writer = ArrowWriter::try_new(file, schema, None).context("create Parquet writer")?;
    writer.write(&batch).context("write Parquet batch")?;
    writer.close().context("close Parquet writer")?;
    Ok(())
}

fn create_avro(path: &Path) -> Result<()> {
    let schema_str = r#"{
        "type": "record",
        "name": "sample",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": null},
            {"name": "active", "type": "boolean"}
        ]
    }"#;
    let schema = apache_avro::Schema::parse_str(schema_str).context("parse Avro schema")?;
    let file = File::create(path).context("create Avro file")?;
    let mut writer = apache_avro::Writer::new(&schema, file);

    let mut record = apache_avro::types::Record::new(&schema).context("create Avro record")?;
    record.put("id", 1i64);
    record.put("name", Some("alice".to_string()));
    record.put("active", true);
    writer.append(record).context("append Avro record")?;

    let mut record = apache_avro::types::Record::new(&schema).context("create Avro record")?;
    record.put("id", 2i64);
    record.put("name", Option::<String>::None);
    record.put("active", false);
    writer.append(record).context("append Avro record")?;

    writer.flush().context("flush Avro writer")?;
    Ok(())
}
