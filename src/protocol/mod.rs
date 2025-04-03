use bytes::{Buf, BufMut};

mod error;
pub mod message;
pub mod r#type;

pub use error::Error;

pub trait Readable: Sized {
    fn read<B: Buf>(buffer: &mut B) -> Self;
}

pub trait ReadableResult: Sized {
    fn read_result<B: Buf>(buffer: &mut B) -> Result<Self, Error>;
}

pub trait ReadableVersion: Sized {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, Error>;
}

pub trait Writable {
    fn write<B: BufMut>(&self, buffer: &mut B);
}
