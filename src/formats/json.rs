use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use serde::de::{DeserializeSeed, IgnoredAny, SeqAccess, Visitor};
use serde_json::Value;

use crate::model::schema::{DataType, Field, Schema};
use crate::util::io::peek_first_non_ws;

const SAMPLE_LIMIT: usize = 1000;
const ARRAY_SAMPLE_LIMIT: usize = 1000;
const MAX_DEPTH: usize = 8;

#[derive(Clone, Debug)]
struct FieldInfo {
    dtype: DataType,
    nullable: bool,
}

pub fn infer_schema(path: &Path) -> Result<Schema> {
    let file = File::open(path).context("open JSON file")?;
    infer_schema_reader(file)
}

pub fn infer_schema_reader<R: Read>(reader: R) -> Result<Schema> {
    let mut reader = BufReader::new(reader);
    let first = peek_first_non_ws(&mut reader)?;

    let mut state = InferState::new();

    if matches!(first, Some(b'[')) {
        sample_json_array(reader, &mut state)?;
    } else {
        let deser = serde_json::Deserializer::from_reader(reader);
        for value in deser.into_iter::<Value>().take(SAMPLE_LIMIT) {
            let value = value.context("parse JSON value")?;
            state.process_record(&value);
        }
    }

    let mut fields: Vec<Field> = state
        .fields
        .into_iter()
        .map(|(name, info)| Field {
            name,
            dtype: info.dtype,
            nullable: info.nullable,
        })
        .collect();
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Schema { fields })
}

fn sample_json_array<R: Read>(reader: BufReader<R>, state: &mut InferState) -> Result<()> {
    let mut deser = serde_json::Deserializer::from_reader(reader);
    let seed = ArraySeed { state };
    seed.deserialize(&mut deser).context("parse JSON array")?;
    Ok(())
}

struct ArraySeed<'a> {
    state: &'a mut InferState,
}

impl<'de> DeserializeSeed<'de> for ArraySeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayVisitor { state: self.state })
    }
}

struct ArrayVisitor<'a> {
    state: &'a mut InferState,
}

impl<'de> Visitor<'de> for ArrayVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(value) = seq.next_element::<Value>()? {
            self.state.process_record(&value);
            if self.state.samples >= SAMPLE_LIMIT {
                while (seq.next_element::<IgnoredAny>()?).is_some() {}
                break;
            }
        }
        Ok(())
    }
}

struct InferState {
    fields: BTreeMap<String, FieldInfo>,
    samples: usize,
}

impl InferState {
    fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            samples: 0,
        }
    }

    fn process_record(&mut self, value: &Value) {
        let mut present = HashSet::new();
        self.infer_value(value, "", 0, &mut present);
        let keys: Vec<String> = self.fields.keys().cloned().collect();
        for key in keys {
            if !present.contains(&key)
                && let Some(info) = self.fields.get_mut(&key)
            {
                info.nullable = true;
            }
        }
        self.samples += 1;
    }

    fn infer_value(
        &mut self,
        value: &Value,
        path: &str,
        depth: usize,
        present: &mut HashSet<String>,
    ) {
        if depth > MAX_DEPTH {
            if !path.is_empty() {
                self.update_field(path, DataType::Unknown, true, present);
            }
            return;
        }

        match value {
            Value::Null => {
                let name = field_name(path);
                self.update_field(&name, DataType::Null, true, present);
            }
            Value::Bool(_) => {
                let name = field_name(path);
                self.update_field(&name, DataType::Bool, false, present);
            }
            Value::Number(num) => {
                let name = field_name(path);
                let dtype = if num.is_i64() || num.is_u64() {
                    DataType::Int
                } else {
                    DataType::Float
                };
                self.update_field(&name, dtype, false, present);
            }
            Value::String(_) => {
                let name = field_name(path);
                self.update_field(&name, DataType::String, false, present);
            }
            Value::Array(items) => {
                let name = field_name(path);
                let mut element_type = DataType::Null;
                for (idx, item) in items.iter().enumerate() {
                    if idx >= ARRAY_SAMPLE_LIMIT {
                        break;
                    }
                    let inferred = Self::value_dtype(item, depth + 1);
                    element_type = DataType::merge(&element_type, &inferred);
                    if let Value::Object(map) = item {
                        for (key, child) in map {
                            let child_path = join_path(path, key);
                            self.infer_value(child, &child_path, depth + 1, present);
                        }
                    }
                }
                let list_type = DataType::List(Box::new(element_type));
                self.update_field(&name, list_type, false, present);
            }
            Value::Object(map) => {
                if depth >= MAX_DEPTH {
                    if !path.is_empty() {
                        self.update_field(path, DataType::Unknown, true, present);
                    }
                    return;
                }
                for (key, child) in map {
                    let child_path = join_path(path, key);
                    self.infer_value(child, &child_path, depth + 1, present);
                }
            }
        }
    }

    fn value_dtype(value: &Value, depth: usize) -> DataType {
        if depth > MAX_DEPTH {
            return DataType::Unknown;
        }
        match value {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::Number(num) => {
                if num.is_i64() || num.is_u64() {
                    DataType::Int
                } else {
                    DataType::Float
                }
            }
            Value::String(_) => DataType::String,
            Value::Array(items) => {
                let mut inner = DataType::Null;
                for (idx, item) in items.iter().enumerate() {
                    if idx >= ARRAY_SAMPLE_LIMIT {
                        break;
                    }
                    let inferred = Self::value_dtype(item, depth + 1);
                    inner = DataType::merge(&inner, &inferred);
                }
                DataType::List(Box::new(inner))
            }
            Value::Object(_) => DataType::Struct,
        }
    }

    fn update_field(
        &mut self,
        name: &str,
        dtype: DataType,
        nullable: bool,
        present: &mut HashSet<String>,
    ) {
        present.insert(name.to_string());
        self.fields
            .entry(name.to_string())
            .and_modify(|info| {
                info.dtype = DataType::merge(&info.dtype, &dtype);
                info.nullable |= nullable;
            })
            .or_insert(FieldInfo {
                dtype,
                nullable: nullable || self.samples > 0,
            });
    }
}

fn join_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{}.{}", parent, child)
    }
}

fn field_name(path: &str) -> String {
    if path.is_empty() {
        "value".to_string()
    } else {
        path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("megrez_json_{name}_{nonce}"))
    }

    #[test]
    fn infer_schema_flattens_fields() {
        let path = temp_path("schema");
        let mut file = File::create(&path).expect("create temp file");
        writeln!(file, "{{\"a\":1,\"b\":null,\"c\":{{\"d\":\"x\"}}}}").unwrap();
        writeln!(file, "{{\"a\":2,\"c\":{{\"d\":\"y\",\"e\":true}}}}").unwrap();
        drop(file);

        let schema = infer_schema(&path).expect("infer schema");
        fs::remove_file(&path).ok();

        let mut fields = schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.dtype.to_string(), f.nullable))
            .collect::<Vec<_>>();
        fields.sort();

        let expected = vec![
            ("a".to_string(), "int".to_string(), false),
            ("b".to_string(), "null".to_string(), true),
            ("c.d".to_string(), "string".to_string(), false),
            ("c.e".to_string(), "bool".to_string(), true),
        ];
        assert_eq!(fields, expected);
    }
}
