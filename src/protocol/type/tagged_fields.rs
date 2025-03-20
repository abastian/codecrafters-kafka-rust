use bytes::{Buf, BufMut, Bytes};

use crate::protocol::{self, Readable, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

#[derive(Debug, Clone)]
pub struct TaggedField {
    key: u32,
    data: Bytes,
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
impl Readable for TaggedField {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let key = read_unsigned_varint(buffer)?;
        let data = buffer.copy_to_bytes(key as usize);
        Ok(Self { key, data })
    }
}
impl Writable for TaggedField {
    fn write(&self, buffer: &mut impl BufMut) {
        write_unsigned_varint(self.key, buffer);
        buffer.put(self.data.clone());
    }
}

#[derive(Debug, Clone)]
pub struct TaggedFields(Vec<TaggedField>);
impl TaggedFields {
    pub fn new(value: Vec<TaggedField>) -> Self {
        Self(value)
    }

    pub fn values(&self) -> &[TaggedField] {
        &self.0
    }

    pub(crate) fn write(buffer: &mut impl BufMut, values: &[TaggedField]) {
        let sz = values.len() as u32;
        let mut data = values.to_vec();
        data.sort_by_key(|tf| tf.key);

        write_unsigned_varint(sz, buffer);
        for tf in data {
            tf.write(buffer);
        }
    }

    #[inline(always)]
    pub(crate) fn write_empty(buffer: &mut impl BufMut) {
        buffer.put_u8(0);
    }
}
impl Readable for TaggedFields {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        let mut result = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            let item = TaggedField::read(buffer)?;
            result.push(item);
        }

        Ok(Self(result))
    }
}
impl Writable for TaggedFields {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, &self.0);
    }
}
