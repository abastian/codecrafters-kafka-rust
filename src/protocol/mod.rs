use bytes::{Buf, BufMut};

mod error;
pub mod message;
pub mod r#type;

pub use error::Error;

pub trait Readable: Sized {
    fn read(buffer: &mut impl Buf) -> Result<Self, Error>;
}

pub trait Writable {
    fn write(&self, buffer: &mut impl BufMut);
}
