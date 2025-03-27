use std::str::Utf8Error;

#[derive(Debug, thiserror::Error, PartialEq, Clone)]
pub enum Error {
    #[error("unsupported version")]
    UnsupportedVersion,

    #[error("illegal argument: {0}")]
    IllegalArgument(&'static str),

    #[error("buffer underflow")]
    BufferUnderflow,

    #[error("utf8 error: {0}")]
    Utf8Error(#[from] Utf8Error),

    #[error("unknown request: {0}")]
    UnknownRequest(i16),

    #[error("io error: {0}")]
    IOError(String),
}
