use std::io::Read;
use std::path::Path;

use anyhow::Result;

use crate::formats::{self, Format, detect::detect_format, detect::detect_format_prefix};
use crate::util::errors::UnsupportedFormatError;
use crate::util::io;

const STDIN_PREFIX_LIMIT: usize = 64 * 1024;

pub fn run(path: &Path, format_override: Option<Format>, limit: Option<usize>) -> Result<()> {
    if io::is_stdin_path(path) {
        let format = if let Some(format) = format_override {
            format
        } else {
            let stdin = std::io::stdin();
            let mut stdin_lock = stdin.lock();
            let prefix = io::read_prefix(&mut stdin_lock, STDIN_PREFIX_LIMIT)?;
            let format = detect_format_prefix(&prefix)?;
            let mut reader = std::io::Cursor::new(prefix).chain(stdin_lock);
            return stream_stdin_cat(format, &mut reader);
        };
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        return stream_stdin_cat(format, &mut reader);
    }

    let format = format_override.unwrap_or(detect_format(path)?);
    match format {
        Format::Json | Format::Csv => io::stream_file(path, &mut std::io::stdout())?,
        Format::Parquet => formats::parquet::cat(path, limit)?,
        Format::Avro => formats::avro::cat(path, limit)?,
        Format::Orc => formats::orc::cat(path, limit)?,
    }
    Ok(())
}

fn stream_stdin_cat(format: Format, reader: &mut impl std::io::Read) -> Result<()> {
    match format {
        Format::Json | Format::Csv => {
            std::io::copy(reader, &mut std::io::stdout())?;
            Ok(())
        }
        Format::Parquet | Format::Avro | Format::Orc => Err(UnsupportedFormatError::new(
            "stdin input is only supported for JSON and CSV",
        )
        .into()),
    }
}
