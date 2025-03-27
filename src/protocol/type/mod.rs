mod array;
mod numeric;
mod string;
mod tagged_fields;

pub use array::{Array, CompactArray};
use bytes::{Buf, BufMut};
pub(crate) use numeric::{read_unsigned_varint, write_unsigned_varint};
pub use numeric::{VarInt, VarLong};
pub use string::{CompactKafkaString, KafkaString};
pub use tagged_fields::{TaggedField, TaggedFields};
use uuid::Uuid;

use super::{Readable, ReadableResult, ReadableVersion, Writable};

impl Readable for Uuid {
    fn read(buffer: &mut impl Buf) -> Uuid {
        Uuid::from_u128(buffer.get_u128())
    }
}
impl Writable for Uuid {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u128(self.as_u128());
    }
}

#[derive(Debug)]
pub struct NullableRecord<T>(Option<T>);
impl<T> NullableRecord<T> {
    pub fn value(&self) -> Option<&T> {
        self.0.as_ref()
    }
}
impl<T> NullableRecord<T>
where
    T: ReadableResult,
{
    pub(crate) fn read_inner(buffer: &mut impl Buf) -> Result<Option<T>, super::Error> {
        if i8::read(buffer) == -1 {
            Ok(None)
        } else {
            Ok(Some(T::read_result(buffer)?))
        }
    }
}
impl<T> NullableRecord<T>
where
    T: ReadableVersion,
{
    pub(crate) fn read_version(
        buffer: &mut impl Buf,
        version: i16,
    ) -> Result<Option<T>, super::Error> {
        if i8::read(buffer) == -1 {
            Ok(None)
        } else {
            Ok(Some(T::read_version(buffer, version)?))
        }
    }
}
impl<T> NullableRecord<T>
where
    T: Writable,
{
    pub(crate) fn write_inner(buffer: &mut impl BufMut, value: Option<&T>) {
        match value {
            Some(value) => {
                0u8.write(buffer);
                value.write(buffer);
            }
            None => (-1i8).write(buffer),
        }
    }
}
impl<T> Clone for NullableRecord<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> From<Option<T>> for NullableRecord<T> {
    fn from(value: Option<T>) -> Self {
        Self(value)
    }
}
impl<T> ReadableResult for NullableRecord<T>
where
    T: ReadableResult,
{
    fn read_result(buffer: &mut impl Buf) -> Result<Self, super::Error> {
        NullableRecord::read_inner(buffer).map(|res| res.into())
    }
}
impl<T> ReadableVersion for NullableRecord<T>
where
    T: ReadableVersion,
{
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, super::Error> {
        NullableRecord::read_version(buffer, version).map(|res| res.into())
    }
}
