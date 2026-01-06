use std::path::Path;

use anyhow::{Result, bail};

use crate::model::schema::Schema;
use crate::util::errors::UnsupportedFormatError;

#[cfg(feature = "orc")]
pub fn infer_schema(_path: &Path) -> Result<Schema> {
    bail!(UnsupportedFormatError::new(
        "ORC support is not implemented in this build"
    ))
}

#[cfg(not(feature = "orc"))]
pub fn infer_schema(_path: &Path) -> Result<Schema> {
    bail!(UnsupportedFormatError::new(
        "ORC support is disabled; rebuild with --features orc"
    ))
}

#[cfg(feature = "orc")]
pub fn cat(_path: &Path, _limit: Option<usize>) -> Result<()> {
    bail!(UnsupportedFormatError::new(
        "ORC support is not implemented in this build"
    ))
}

#[cfg(not(feature = "orc"))]
pub fn cat(_path: &Path, _limit: Option<usize>) -> Result<()> {
    bail!(UnsupportedFormatError::new(
        "ORC support is disabled; rebuild with --features orc"
    ))
}
