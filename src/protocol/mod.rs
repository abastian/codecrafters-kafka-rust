use bytes::{Buf, BufMut};

mod error;
pub mod message;
pub mod r#type;

pub use error::Error;

pub trait Readable: Sized {
    fn read(buffer: &mut impl Buf) -> Self;
}

pub trait ReadableResult: Sized {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, Error>;
}

pub trait ReadableVersion: Sized {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, Error>;
}

pub trait Writable {
    fn write(&self, buffer: &mut impl BufMut);
}
