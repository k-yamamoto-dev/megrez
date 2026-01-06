use std::io::Write;

use anyhow::Result;
use serde_json::Value;

pub fn write_line<W: Write>(writer: &mut W, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}
