use bytes::Bytes;

use crate::protocol::{self, Readable, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

pub struct String(pub Bytes);
impl Readable for String {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = buffer.get_i16();
        if sz < 0 {
            Err(protocol::Error::IllegalArgument(
                "Invalid String, negative size",
            ))
        } else {
            let value = buffer.copy_to_bytes(sz as usize);
            Ok(Self(value))
        }
    }
}
impl Writable for String {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.0.len() as i16;
        buffer.put_i16(sz);
        buffer.put(self.0.clone());
    }
}

pub struct CompactString(pub Bytes);
impl Readable for CompactString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            Err(protocol::Error::IllegalArgument(
                "Invalid CompactString, negative size",
            ))
        } else {
            let sz = sz as usize - 1;
            let value = buffer.copy_to_bytes(sz);
            Ok(Self(value))
        }
    }
}
impl Writable for CompactString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.0.len();
        let sz = sz as u32 + 1;
        write_unsigned_varint(sz, buffer);
        buffer.put(self.0.clone());
    }
}

pub struct NullableString(pub Option<Bytes>);
impl Readable for NullableString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = buffer.get_i16();
        if sz < 0 {
            return Ok(Self(None));
        }
        let sz = sz as usize;
        let value = buffer.copy_to_bytes(sz);
        Ok(Self(Some(value)))
    }
}
impl Writable for NullableString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match &self.0 {
            Some(data) => {
                let sz = data.len() as i16;
                buffer.put_i16(sz);
                buffer.put(data.clone());
            }
            None => {
                buffer.put_i16(-1);
            }
        }
    }
}
