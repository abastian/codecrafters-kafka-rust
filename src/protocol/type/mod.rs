mod array;
mod numeric;
mod string;
mod tagged_fields;

pub use array::{Array, CompactArray};
pub(crate) use numeric::{
    read_unsigned_varint, read_unsigned_varlong, write_unsigned_varint, write_unsigned_varlong,
};
pub use numeric::{Boolean, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, VarInt, VarLong};
pub use string::{
    CompactKafkaString, CompactNullableKafkaString, KafkaString, NullableKafkaString,
};
pub use tagged_fields::{TaggedField, TaggedFields};

use super::{Readable, Writable};

pub struct KafkaUuid(pub uuid::Uuid);
impl Readable for KafkaUuid {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, super::Error> {
        Ok(Self(uuid::Uuid::from_u128(buffer.get_u128())))
    }
}
impl Writable for KafkaUuid {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        buffer.put_u128(self.0.as_u128());
    }
}

pub struct NullableRecord<T>(pub Option<T>);
impl<T> Readable for NullableRecord<T>
where
    T: Readable,
{
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, super::Error> {
        if buffer.get_i8() == -1 {
            Ok(Self(None))
        } else {
            Ok(Self(Some(T::read(buffer)?)))
        }
    }
}
impl<T> Writable for NullableRecord<T>
where
    T: Writable,
{
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self.0 {
            Some(ref value) => {
                buffer.put_u8(0);
                value.write(buffer);
            }
            None => buffer.put_i8(-1),
        }
    }
}
