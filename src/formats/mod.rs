pub mod avro;
pub mod csv;
pub mod detect;
pub mod json;
pub mod orc;
pub mod parquet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Csv,
    Parquet,
    Avro,
    Orc,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Json => "JSON",
            Format::Csv => "CSV",
            Format::Parquet => "PARQUET",
            Format::Avro => "AVRO",
            Format::Orc => "ORC",
        }
    }
}
