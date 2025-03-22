use bytes::{Buf, BufMut, Bytes};

use crate::protocol::{self, Readable, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

#[derive(Debug, Clone)]
pub struct KafkaString(Bytes);
impl KafkaString {
    pub fn value(&self) -> &Bytes {
        &self.0
    }

    pub fn as_str(&self) -> Result<&str, protocol::Error> {
        std::str::from_utf8(self.0.as_ref()).map_err(|e| e.into())
    }

    pub(crate) fn write(buffer: &mut impl BufMut, value: &[u8]) {
        let sz = value.len() as i16;
        buffer.put_i16(sz);
        buffer.put(value);
    }
}
impl From<&Bytes> for KafkaString {
    fn from(value: &Bytes) -> Self {
        Self(value.clone())
    }
}
impl From<&str> for KafkaString {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}
impl Readable for KafkaString {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
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
impl Writable for KafkaString {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0.as_ref());
    }
}

#[derive(Debug, Clone)]
pub struct CompactKafkaString(Bytes);
impl CompactKafkaString {
    pub fn value(&self) -> &Bytes {
        &self.0
    }
    pub fn as_str(&self) -> Result<&str, protocol::Error> {
        std::str::from_utf8(self.0.as_ref()).map_err(|e| e.into())
    }

    pub(crate) fn write(buffer: &mut impl BufMut, value: &[u8]) {
        let sz = value.len() as u32 + 1;
        write_unsigned_varint(sz, buffer);
        buffer.put(value);
    }
}
impl From<&Bytes> for CompactKafkaString {
    fn from(value: &Bytes) -> Self {
        Self(value.clone())
    }
}
impl From<&str> for CompactKafkaString {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}
impl Readable for CompactKafkaString {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
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
impl Writable for CompactKafkaString {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0.as_ref());
    }
}

#[derive(Debug, Clone)]
pub struct NullableKafkaString(Option<Bytes>);
impl NullableKafkaString {
    pub fn value(&self) -> Option<&Bytes> {
        self.0.as_ref()
    }

    pub fn as_str(&self) -> Option<Result<&str, protocol::Error>> {
        self.0
            .as_ref()
            .map(|b| std::str::from_utf8(b.as_ref()).map_err(|e| e.into()))
    }

    #[inline(always)]
    pub(crate) fn write_empty(buffer: &mut impl BufMut) {
        buffer.put_i16(-1);
    }
}
impl From<Option<&Bytes>> for NullableKafkaString {
    fn from(value: Option<&Bytes>) -> Self {
        Self(value.cloned())
    }
}
impl From<Option<&str>> for NullableKafkaString {
    fn from(value: Option<&str>) -> Self {
        Self(value.map(|s| Bytes::copy_from_slice(s.as_bytes())))
    }
}
impl Readable for NullableKafkaString {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let sz = buffer.get_i16();
        if sz < 0 {
            return Ok(Self(None));
        }
        let sz = sz as usize;
        let value = buffer.copy_to_bytes(sz);
        Ok(Self(Some(value)))
    }
}
impl Writable for NullableKafkaString {
    fn write(&self, buffer: &mut impl BufMut) {
        match &self.0 {
            Some(data) => {
                KafkaString::write(buffer, data.as_ref());
            }
            None => {
                Self::write_empty(buffer);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactNullableKafkaString(Option<Bytes>);
impl CompactNullableKafkaString {
    pub fn value(&self) -> Option<&Bytes> {
        self.0.as_ref()
    }

    pub fn as_str(&self) -> Option<Result<&str, protocol::Error>> {
        self.0
            .as_ref()
            .map(|b| std::str::from_utf8(b.as_ref()).map_err(|e| e.into()))
    }

    #[inline(always)]
    pub(crate) fn write_empty(buffer: &mut impl BufMut) {
        buffer.put_u8(0);
    }
}
impl From<Option<&Bytes>> for CompactNullableKafkaString {
    fn from(value: Option<&Bytes>) -> Self {
        Self(value.cloned())
    }
}
impl From<Option<&str>> for CompactNullableKafkaString {
    fn from(value: Option<&str>) -> Self {
        Self(value.map(|s| Bytes::copy_from_slice(s.as_bytes())))
    }
}
impl Readable for CompactNullableKafkaString {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self(None));
        }
        let sz = sz as usize - 1;
        let value = buffer.copy_to_bytes(sz);
        Ok(Self(Some(value)))
    }
}
impl Writable for CompactNullableKafkaString {
    fn write(&self, buffer: &mut impl BufMut) {
        match &self.0 {
            Some(data) => {
                CompactKafkaString::write(buffer, data.as_ref());
            }
            None => {
                Self::write_empty(buffer);
            }
        }
    }
}
