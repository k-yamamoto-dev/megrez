use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::formats::Format;
use crate::util::errors::UnsupportedFormatError;
use crate::util::io::peek_first_non_ws;

const MAGIC_PARQUET: &[u8; 4] = b"PAR1";
const MAGIC_AVRO: &[u8; 4] = b"Obj\x01";
const MAGIC_ORC: &[u8; 3] = b"ORC";

pub fn detect_format(path: &Path) -> Result<Format> {
    if !path.exists() {
        bail!("input file does not exist");
    }
    let mut tried = Vec::new();

    if let Ok(format) = detect_by_magic(path) {
        return Ok(format);
    }

    if let Some(format) = detect_by_extension(path) {
        return Ok(format);
    }

    tried.push("magic bytes");
    tried.push("file extension");

    tried.push("content sniff");
    if let Ok(format) = detect_by_content(path) {
        return Ok(format);
    }

    let details = format!("format detection failed (tried: {})", tried.join(", "));
    bail!(UnsupportedFormatError::new(details));
}

pub fn detect_format_prefix(prefix: &[u8]) -> Result<Format> {
    if prefix.len() >= 4 {
        if &prefix[..4] == MAGIC_PARQUET {
            return Ok(Format::Parquet);
        }
        if &prefix[..4] == MAGIC_AVRO {
            return Ok(Format::Avro);
        }
    }
    if prefix.len() >= 3 && &prefix[..3] == MAGIC_ORC {
        return Ok(Format::Orc);
    }
    if let Some(byte) = prefix.iter().copied().find(|b| !b.is_ascii_whitespace()) {
        if byte == b'{' || byte == b'[' {
            return Ok(Format::Json);
        }
        return Ok(Format::Csv);
    }
    bail!(UnsupportedFormatError::new(
        "stdin format detection failed (empty or whitespace-only input)"
    ))
}

fn detect_by_magic(path: &Path) -> Result<Format> {
    let mut file = File::open(path).context("open file for format detection")?;
    let mut buf = [0u8; 4];
    let read = file.read(&mut buf).context("read magic bytes")?;
    if read >= 4 {
        if &buf == MAGIC_PARQUET {
            return Ok(Format::Parquet);
        }
        if &buf == MAGIC_AVRO {
            return Ok(Format::Avro);
        }
    }
    if read >= 3 && &buf[..3] == MAGIC_ORC {
        return Ok(Format::Orc);
    }

    if has_parquet_footer(&mut file)? {
        return Ok(Format::Parquet);
    }

    bail!("no magic match")
}

fn has_parquet_footer(file: &mut File) -> Result<bool> {
    let len = file.metadata().context("read file metadata")?.len();
    if len < 8 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-4))
        .context("seek parquet footer")?;
    let mut buf = [0u8; 4];
    let read = file.read(&mut buf).context("read parquet footer")?;
    Ok(read == 4 && &buf == MAGIC_PARQUET)
}

fn detect_by_extension(path: &Path) -> Option<Format> {
    let ext = path.extension()?.to_string_lossy().to_ascii_lowercase();
    match ext.as_str() {
        "json" | "jsonl" | "ndjson" => Some(Format::Json),
        "csv" => Some(Format::Csv),
        "parquet" => Some(Format::Parquet),
        "avro" => Some(Format::Avro),
        "orc" => Some(Format::Orc),
        _ => None,
    }
}

fn detect_by_content(path: &Path) -> Result<Format> {
    let file = File::open(path).context("open file for content detection")?;
    let mut reader = BufReader::new(file);
    let first = peek_first_non_ws(&mut reader)?;

    if let Some(byte) = first
        && (byte == b'{' || byte == b'[')
    {
        return Ok(Format::Json);
    }

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let mut record = csv::StringRecord::new();
    if csv_reader.read_record(&mut record)? && !record.is_empty() {
        return Ok(Format::Csv);
    }

    bail!("unrecognized content")
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
        std::env::temp_dir().join(format!("megrez_{name}_{nonce}"))
    }

    fn temp_path_with_ext(stem: &str, ext: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("megrez_{stem}_{nonce}.{ext}"))
    }

    #[test]
    fn detect_by_magic_parquet() {
        let path = temp_path("parquet");
        let mut file = File::create(&path).expect("create temp file");
        file.write_all(b"PAR1xxxx").expect("write magic");
        let format = detect_format(&path).expect("detect format");
        fs::remove_file(&path).ok();
        assert_eq!(format, Format::Parquet);
    }

    #[test]
    fn detect_by_footer_parquet() {
        let path = temp_path("parquet_footer");
        let mut file = File::create(&path).expect("create temp file");
        file.write_all(b"xxxxPAR1").expect("write footer");
        let format = detect_format(&path).expect("detect format");
        fs::remove_file(&path).ok();
        assert_eq!(format, Format::Parquet);
    }

    #[test]
    fn detect_by_magic_avro() {
        let path = temp_path("avro");
        let mut file = File::create(&path).expect("create temp file");
        file.write_all(b"Obj\x01xxxx").expect("write magic");
        let format = detect_format(&path).expect("detect format");
        fs::remove_file(&path).ok();
        assert_eq!(format, Format::Avro);
    }

    #[test]
    fn detect_by_extension_csv() {
        let path = temp_path_with_ext("data", "csv");
        let mut file = File::create(&path).expect("create temp file");
        file.write_all(b"a,b\n1,2\n").expect("write data");
        let format = detect_format(&path).expect("detect format");
        fs::remove_file(&path).ok();
        assert_eq!(format, Format::Csv);
    }

    #[test]
    fn detect_by_content_json() {
        let path = temp_path("content");
        let mut file = File::create(&path).expect("create temp file");
        file.write_all(b"{\"a\":1}").expect("write json");
        let format = detect_format(&path).expect("detect format");
        fs::remove_file(&path).ok();
        assert_eq!(format, Format::Json);
    }
}
