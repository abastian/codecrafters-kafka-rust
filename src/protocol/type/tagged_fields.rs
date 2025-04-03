use bytes::{Buf, BufMut, Bytes};

use crate::protocol::{self, ReadableResult, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

#[derive(Debug, Clone)]
pub struct TaggedField {
    pub(crate) key: u32,
    pub(crate) data: Bytes,
}
impl TaggedField {
    pub fn new(key: u32, data: Bytes) -> Self {
        Self { key, data }
    }

    pub fn key(&self) -> u32 {
        self.key
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}
impl ReadableResult for TaggedField {
    fn read_result<B: Buf>(buffer: &mut B) -> Result<Self, protocol::Error> {
        let key = read_unsigned_varint(buffer)?;
        let data = buffer.copy_to_bytes(key as usize);
        Ok(Self { key, data })
    }
}
impl Writable for TaggedField {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        write_unsigned_varint(buffer, self.key);
        buffer.put_slice(self.data.as_ref());
    }
}

#[derive(Debug, Clone)]
pub struct TaggedFields(Vec<TaggedField>);
impl TaggedFields {
    pub fn new(value: Vec<TaggedField>) -> Self {
        Self(value)
    }

    pub fn value(&self) -> &[TaggedField] {
        &self.0
    }

    pub(crate) fn read_result_inner<B: Buf>(
        buffer: &mut B,
    ) -> Result<Vec<TaggedField>, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        let mut result = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            let item = TaggedField::read_result(buffer)?;
            result.push(item);
        }

        Ok(result)
    }

    pub(crate) fn write_inner<B: BufMut>(buffer: &mut B, values: &[TaggedField]) {
        let sz = values.len() as u32;
        let mut data = values.to_vec();
        data.sort_by_key(|tf| tf.key);

        write_unsigned_varint(buffer, sz);
        for tf in data {
            tf.write(buffer);
        }
    }

    pub(crate) fn write_empty<B: BufMut>(buffer: &mut B) {
        0u8.write(buffer);
    }
}
impl ReadableResult for TaggedFields {
    fn read_result<B: Buf>(buffer: &mut B) -> Result<Self, protocol::Error> {
        Self::read_result_inner(buffer).map(Self)
    }
}
impl Writable for TaggedFields {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        Self::write_inner(buffer, self.value());
    }
}
