use crate::protocol::{self, Readable, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

pub struct KafkaString(pub String);
impl Readable for KafkaString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = buffer.get_i16();
        if sz < 0 {
            Err(protocol::Error::IllegalArgument(
                "Invalid String, negative size",
            ))
        } else {
            let value =
                std::str::from_utf8(buffer.copy_to_bytes(sz as usize).as_ref())?.to_string();
            Ok(Self(value))
        }
    }
}
impl Writable for KafkaString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.0.len() as i16;
        buffer.put_i16(sz);
        buffer.put(self.0.as_bytes());
    }
}

pub struct CompactKafkaString(pub String);
impl Readable for CompactKafkaString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            Err(protocol::Error::IllegalArgument(
                "Invalid CompactString, negative size",
            ))
        } else {
            let sz = sz as usize - 1;
            let value = std::str::from_utf8(buffer.copy_to_bytes(sz).as_ref())?.to_string();
            Ok(Self(value))
        }
    }
}
impl Writable for CompactKafkaString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.0.len() as u32 + 1;
        write_unsigned_varint(sz, buffer);
        buffer.put(self.0.as_bytes());
    }
}

pub struct NullableKafkaString(pub Option<String>);
impl Readable for NullableKafkaString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = buffer.get_i16();
        if sz < 0 {
            return Ok(Self(None));
        }
        let sz = sz as usize;
        let value = std::str::from_utf8(buffer.copy_to_bytes(sz).as_ref())?.to_string();
        Ok(Self(Some(value)))
    }
}
impl Writable for NullableKafkaString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match &self.0 {
            Some(data) => {
                let sz = data.len() as i16;
                buffer.put_i16(sz);
                buffer.put(data.as_bytes());
            }
            None => {
                buffer.put_i16(-1);
            }
        }
    }
}

pub struct CompactNullableKafkaString(pub Option<String>);
impl Readable for CompactNullableKafkaString {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self(None));
        }
        let sz = sz as usize - 1;
        let value = std::str::from_utf8(buffer.copy_to_bytes(sz).as_ref())?.to_string();
        Ok(Self(Some(value)))
    }
}
impl Writable for CompactNullableKafkaString {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match &self.0 {
            Some(data) => {
                let sz = data.len() as u32 + 1;
                write_unsigned_varint(sz, buffer);
                buffer.put(data.as_bytes());
            }
            None => {
                buffer.put_u8(0);
            }
        }
    }
}
