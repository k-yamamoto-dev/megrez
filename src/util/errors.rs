use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct UnsupportedFormatError {
    message: String,
}

impl UnsupportedFormatError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
