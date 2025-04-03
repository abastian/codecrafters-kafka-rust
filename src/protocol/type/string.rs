use bytes::{Buf, BufMut, Bytes};

use crate::protocol::{self, Readable, ReadableResult, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

#[derive(Debug, Clone)]
pub struct KafkaString(Option<Bytes>);
impl KafkaString {
    pub fn value(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }

    pub(crate) fn write_none<B: BufMut>(buffer: &mut B) {
        (-1i16).write(buffer);
    }

    pub(crate) fn read_inner<B: Buf>(buffer: &mut B) -> Option<Bytes> {
        let sz = i16::read(buffer);
        if sz < 0 {
            return None;
        }

        let value = buffer.copy_to_bytes(sz as usize);
        Some(value)
    }

    pub(crate) fn write_inner<B: BufMut>(buffer: &mut B, data: Option<&[u8]>) {
        if let Some(data) = data {
            let sz = data.len() as i16;
            sz.write(buffer);
            buffer.put_slice(data);
        } else {
            Self::write_none(buffer);
        }
    }
}
impl From<Option<Bytes>> for KafkaString {
    fn from(value: Option<Bytes>) -> Self {
        Self(value)
    }
}
impl From<Option<&str>> for KafkaString {
    fn from(value: Option<&str>) -> Self {
        Self(value.map(|s| Bytes::copy_from_slice(s.as_bytes())))
    }
}
impl Readable for KafkaString {
    fn read<B: Buf>(buffer: &mut B) -> Self {
        Self::read_inner(buffer).into()
    }
}
impl Writable for KafkaString {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        Self::write_inner(buffer, self.value());
    }
}

#[derive(Debug, Clone)]
pub struct CompactKafkaString(Option<Bytes>);
impl CompactKafkaString {
    pub fn value(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }

    pub(crate) fn write_none<B: BufMut>(buffer: &mut B) {
        0u8.write(buffer);
    }

    pub(crate) fn read_result_inner<B: Buf>(
        buffer: &mut B,
    ) -> Result<Option<Bytes>, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(None);
        }
        if sz == 1 {
            return Ok(Some(Bytes::new()));
        }

        let sz = sz as usize - 1;
        let value = buffer.copy_to_bytes(sz);
        Ok(Some(value))
    }

    pub(crate) fn write_inner<B: BufMut>(buffer: &mut B, value: Option<&[u8]>) {
        if let Some(value) = value {
            let sz = value.len() as u32 + 1;
            write_unsigned_varint(buffer, sz);
            buffer.put_slice(value);
        } else {
            Self::write_none(buffer);
        }
    }
}
impl From<Option<Bytes>> for CompactKafkaString {
    fn from(value: Option<Bytes>) -> Self {
        Self(value)
    }
}
impl From<Option<&str>> for CompactKafkaString {
    fn from(value: Option<&str>) -> Self {
        Self(value.map(|s| Bytes::copy_from_slice(s.as_bytes())))
    }
}
impl ReadableResult for CompactKafkaString {
    fn read_result<B: Buf>(buffer: &mut B) -> Result<Self, protocol::Error> {
        Self::read_result_inner(buffer).map(Self)
    }
}
impl Writable for CompactKafkaString {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        Self::write_inner(buffer, self.value());
    }
}
