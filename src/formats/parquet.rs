use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value as JsonValue;

use crate::model::schema::{DataType, Field, Schema};
use crate::render::jsonl;

pub fn infer_schema(path: &Path) -> Result<Schema> {
    let file = File::open(path).context("open Parquet file")?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("read Parquet metadata")?;
    let arrow_schema = builder.schema();

    let fields = arrow_schema
        .fields()
        .iter()
        .map(|field| map_field(field))
        .collect();

    Ok(Schema { fields })
}

pub fn cat(path: &Path, limit: Option<usize>) -> Result<()> {
    let file = File::open(path).context("open Parquet file")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("read Parquet metadata")?
        .with_batch_size(1024);
    let reader = builder.build().context("build Parquet reader")?;
    let mut out = std::io::stdout();
    let mut count = 0usize;

    for batch in reader {
        let batch = batch.context("read Parquet record batch")?;
        for row in 0..batch.num_rows() {
            let json = batch_row_to_json(&batch, row);
            jsonl::write_line(&mut out, &json)?;
            count += 1;
            if let Some(limit) = limit
                && count >= limit
            {
                return Ok(());
            }
        }
    }

    Ok(())
}

fn map_field(field: &ArrowField) -> Field {
    let dtype = map_arrow_type(field.data_type());
    Field {
        name: field.name().clone(),
        dtype,
        nullable: field.is_nullable(),
    }
}

fn map_arrow_type(arrow: &ArrowType) -> DataType {
    match arrow {
        ArrowType::Boolean => DataType::Bool,
        ArrowType::Int8
        | ArrowType::Int16
        | ArrowType::Int32
        | ArrowType::Int64
        | ArrowType::UInt8
        | ArrowType::UInt16
        | ArrowType::UInt32
        | ArrowType::UInt64 => DataType::Int,
        ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => DataType::Float,
        ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::String,
        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::FixedSizeBinary(_) => {
            DataType::Bytes
        }
        ArrowType::Timestamp(_, _) => DataType::Timestamp,
        ArrowType::Date32 | ArrowType::Date64 => DataType::Date,
        ArrowType::Struct(_) => DataType::Struct,
        ArrowType::List(field) | ArrowType::LargeList(field) => {
            let inner = map_arrow_type(field.data_type());
            DataType::List(Box::new(inner))
        }
        _ => DataType::Unknown,
    }
}

fn batch_row_to_json(batch: &RecordBatch, row: usize) -> JsonValue {
    let mut map = serde_json::Map::new();
    let schema: Arc<ArrowSchema> = batch.schema();
    for (idx, field) in schema.fields().iter().enumerate() {
        let array = batch.column(idx).as_ref();
        let value = array_value(array, row);
        map.insert(field.name().clone(), value);
    }
    JsonValue::Object(map)
}

fn array_value(array: &dyn Array, row: usize) -> JsonValue {
    if array.is_null(row) {
        return JsonValue::Null;
    }

    match array.data_type() {
        ArrowType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            JsonValue::Bool(array.value(row))
        }
        ArrowType::Int8 => {
            let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::UInt8 => {
            let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::UInt16 => {
            let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            JsonValue::String(bytes_to_hex(array.value(row)))
        }
        ArrowType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            JsonValue::String(bytes_to_hex(array.value(row)))
        }
        ArrowType::FixedSizeBinary(_) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            JsonValue::String(bytes_to_hex(array.value(row)))
        }
        ArrowType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            JsonValue::from(array.value(row))
        }
        ArrowType::Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = serde_json::Map::new();
            for (idx, field) in array.fields().iter().enumerate() {
                let child = array.column(idx);
                let value = array_value(child.as_ref(), row);
                map.insert(field.name().clone(), value);
            }
            JsonValue::Object(map)
        }
        ArrowType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let child = array.value(row);
            list_array_to_json(child.as_ref())
        }
        ArrowType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let child = array.value(row);
            list_array_to_json(child.as_ref())
        }
        _ => JsonValue::Null,
    }
}

fn list_array_to_json(array: &dyn Array) -> JsonValue {
    let mut values = Vec::with_capacity(array.len());
    for idx in 0..array.len() {
        values.push(array_value(array, idx));
    }
    JsonValue::Array(values)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}
