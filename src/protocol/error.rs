use std::str::Utf8Error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported version")]
    UnsupportedVersion,

    #[error("illegal argument: {0}")]
    IllegalArgument(&'static str),

    #[error("utf8 error: {0}")]
    Utf8Error(#[from] Utf8Error),
}
