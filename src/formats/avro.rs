use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value;
use serde_json::Value as JsonValue;

use crate::model::schema::{DataType, Field, Schema};
use crate::render::jsonl;

pub fn infer_schema(path: &Path) -> Result<Schema> {
    let file = File::open(path).context("open Avro file")?;
    let reader = apache_avro::Reader::new(file).context("read Avro container")?;
    let schema = reader.writer_schema().clone();

    let fields = match schema {
        AvroSchema::Record(record) => record
            .fields
            .iter()
            .map(|field| {
                let (dtype, nullable) = schema_to_dtype(&field.schema);
                Field {
                    name: field.name.clone(),
                    dtype,
                    nullable,
                }
            })
            .collect(),
        other => {
            let (dtype, nullable) = schema_to_dtype(&other);
            vec![Field {
                name: "value".to_string(),
                dtype,
                nullable,
            }]
        }
    };

    Ok(Schema { fields })
}

pub fn cat(path: &Path, limit: Option<usize>) -> Result<()> {
    let file = File::open(path).context("open Avro file")?;
    let reader = apache_avro::Reader::new(file).context("read Avro container")?;
    let mut out = std::io::stdout();
    let mut count = 0usize;

    for record in reader {
        let value = record.context("read Avro record")?;
        let json = avro_value_to_json(&value);
        jsonl::write_line(&mut out, &json)?;
        count += 1;
        if let Some(limit) = limit
            && count >= limit
        {
            break;
        }
    }

    Ok(())
}

fn schema_to_dtype(schema: &AvroSchema) -> (DataType, bool) {
    match schema {
        AvroSchema::Null => (DataType::Null, true),
        AvroSchema::Boolean => (DataType::Bool, false),
        AvroSchema::Int | AvroSchema::Long => (DataType::Int, false),
        AvroSchema::Float | AvroSchema::Double => (DataType::Float, false),
        AvroSchema::Bytes | AvroSchema::Fixed(_) => (DataType::Bytes, false),
        AvroSchema::String | AvroSchema::Enum(_) | AvroSchema::Uuid => (DataType::String, false),
        AvroSchema::Decimal(_) => (DataType::String, false),
        AvroSchema::Date => (DataType::Date, false),
        AvroSchema::TimeMillis
        | AvroSchema::TimeMicros
        | AvroSchema::TimestampMillis
        | AvroSchema::TimestampMicros
        | AvroSchema::LocalTimestampMillis
        | AvroSchema::LocalTimestampMicros => (DataType::Timestamp, false),
        AvroSchema::Array(item) => {
            let (inner, _) = schema_to_dtype(item.as_ref());
            (DataType::List(Box::new(inner)), false)
        }
        AvroSchema::Map(_) => (DataType::Struct, false),
        AvroSchema::Record(_) => (DataType::Struct, false),
        AvroSchema::Duration => (DataType::String, false),
        AvroSchema::Union(union) => {
            let mut nullable = false;
            let mut dtype = DataType::Unknown;
            for variant in union.variants() {
                if matches!(variant, AvroSchema::Null) {
                    nullable = true;
                    continue;
                }
                let (next, _) = schema_to_dtype(variant);
                dtype = DataType::merge(&dtype, &next);
            }
            (dtype, nullable)
        }
        AvroSchema::Ref { .. } => (DataType::Unknown, false),
    }
}

fn avro_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(v) => JsonValue::Bool(*v),
        Value::Int(v) => JsonValue::from(*v),
        Value::Long(v) => JsonValue::from(*v),
        Value::Float(v) => JsonValue::from(*v),
        Value::Double(v) => JsonValue::from(*v),
        Value::Bytes(bytes) => JsonValue::String(bytes_to_hex(bytes)),
        Value::Fixed(_, bytes) => JsonValue::String(bytes_to_hex(bytes)),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Enum(_, v) => JsonValue::String(v.clone()),
        Value::Uuid(v) => JsonValue::String(v.to_string()),
        Value::Date(v) => JsonValue::from(*v),
        Value::Decimal(decimal) => JsonValue::String(format!("{decimal:?}")),
        Value::TimeMillis(v) => JsonValue::from(*v),
        Value::TimeMicros(v) => JsonValue::from(*v),
        Value::TimestampMillis(v) => JsonValue::from(*v),
        Value::TimestampMicros(v) => JsonValue::from(*v),
        Value::LocalTimestampMillis(v) => JsonValue::from(*v),
        Value::LocalTimestampMicros(v) => JsonValue::from(*v),
        Value::Duration(duration) => JsonValue::String(format!("{duration:?}")),
        Value::Array(items) => JsonValue::Array(items.iter().map(avro_value_to_json).collect()),
        Value::Map(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut out = serde_json::Map::new();
            for (key, val) in entries {
                out.insert(key.clone(), avro_value_to_json(val));
            }
            JsonValue::Object(out)
        }
        Value::Record(fields) => {
            let mut out = serde_json::Map::new();
            for (name, val) in fields {
                out.insert(name.clone(), avro_value_to_json(val));
            }
            JsonValue::Object(out)
        }
        Value::Union(_, boxed) => avro_value_to_json(boxed.as_ref()),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}
