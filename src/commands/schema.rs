use std::io::Read;
use std::path::Path;

use anyhow::Result;

use crate::formats::{self, Format, detect::detect_format, detect::detect_format_prefix};
use crate::render::schema_text;
use crate::util::errors::UnsupportedFormatError;
use crate::util::io;

const STDIN_PREFIX_LIMIT: usize = 64 * 1024;

pub fn run(
    path: &Path,
    format_override: Option<Format>,
    show_format_name: bool,
    show_columns: bool,
) -> Result<()> {
    if io::is_stdin_path(path) {
        if let Some(format) = format_override {
            let stdin = std::io::stdin();
            let reader = stdin.lock();
            return render_schema_from_reader(format, reader, show_format_name, show_columns);
        }
        let stdin = std::io::stdin();
        let mut stdin_lock = stdin.lock();
        let prefix = io::read_prefix(&mut stdin_lock, STDIN_PREFIX_LIMIT)?;
        let format = detect_format_prefix(&prefix)?;
        let reader = std::io::Cursor::new(prefix).chain(stdin_lock);
        return render_schema_from_reader(format, reader, show_format_name, show_columns);
    }

    let format = format_override.unwrap_or(detect_format(path)?);
    let schema = match format {
        Format::Json => formats::json::infer_schema(path)?,
        Format::Csv => formats::csv::infer_schema(path)?,
        Format::Parquet => formats::parquet::infer_schema(path)?,
        Format::Avro => formats::avro::infer_schema(path)?,
        Format::Orc => formats::orc::infer_schema(path)?,
    };
    schema_text::render(
        &schema,
        format,
        show_format_name,
        show_columns,
        &mut std::io::stdout(),
    )?;
    Ok(())
}

fn render_schema_from_reader<R: Read>(
    format: Format,
    reader: R,
    show_format_name: bool,
    show_columns: bool,
) -> Result<()> {
    let schema = match format {
        Format::Json => formats::json::infer_schema_reader(reader)?,
        Format::Csv => formats::csv::infer_schema_reader(reader)?,
        Format::Parquet | Format::Avro | Format::Orc => {
            return Err(UnsupportedFormatError::new(
                "stdin input is only supported for JSON and CSV",
            )
            .into());
        }
    };
    schema_text::render(
        &schema,
        format,
        show_format_name,
        show_columns,
        &mut std::io::stdout(),
    )?;
    Ok(())
}
