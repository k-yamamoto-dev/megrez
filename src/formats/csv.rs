use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};

use crate::model::schema::{DataType, Field, Schema};

const SAMPLE_LIMIT: usize = 1000;

pub fn infer_schema(path: &Path) -> Result<Schema> {
    let file = File::open(path).context("open CSV file")?;
    infer_schema_reader(file)
}

pub fn infer_schema_reader<R: Read>(reader: R) -> Result<Schema> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let headers = reader.headers().context("read CSV headers")?.clone();

    let mut dtypes = vec![DataType::Null; headers.len()];
    let mut nullable = vec![false; headers.len()];

    for (idx, record) in reader.records().enumerate() {
        if idx >= SAMPLE_LIMIT {
            break;
        }
        let record = record.context("read CSV record")?;
        for col in 0..headers.len() {
            let value = record.get(col).unwrap_or("");
            let inferred = infer_scalar(value);
            if matches!(inferred, DataType::Null) {
                nullable[col] = true;
            }
            dtypes[col] = DataType::merge(&dtypes[col], &inferred);
        }
    }

    let mut fields = Vec::with_capacity(headers.len());
    for (idx, name) in headers.iter().enumerate() {
        fields.push(Field {
            name: name.to_string(),
            dtype: dtypes[idx].clone(),
            nullable: nullable[idx],
        });
    }

    Ok(Schema { fields })
}

fn infer_scalar(value: &str) -> DataType {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return DataType::Null;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower == "true" || lower == "false" {
        return DataType::Bool;
    }
    if trimmed.parse::<i64>().is_ok() {
        return DataType::Int;
    }
    if trimmed.parse::<f64>().is_ok() {
        return DataType::Float;
    }
    DataType::String
}
