use std::io::Write;

use anyhow::Result;

use crate::formats::Format;
use crate::model::schema::Schema;

pub fn render<W: Write>(
    schema: &Schema,
    format: Format,
    show_format_name: bool,
    show_columns: bool,
    writer: &mut W,
) -> Result<()> {
    if show_format_name {
        writeln!(writer, "format: {}", format.as_str())?;
    }
    if show_columns {
        writeln!(writer, "name\ttype\tnullable")?;
    }
    for field in &schema.fields {
        writeln!(
            writer,
            "{}\t{}\t{}",
            field.name, field.dtype, field.nullable
        )?;
    }
    Ok(())
}
