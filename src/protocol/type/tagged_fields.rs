use bytes::Bytes;

use crate::protocol::{self, Readable, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

pub struct TaggedField {
    pub key: u32,
    pub data: Bytes,
}
impl Readable for TaggedField {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let key = read_unsigned_varint(buffer)?;
        let data = buffer.copy_to_bytes(key as usize);
        Ok(Self { key, data })
    }
}
impl Writable for TaggedField {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        write_unsigned_varint(self.key, buffer);
        buffer.put(self.data.clone());
    }
}

pub type TaggedFields = Vec<TaggedField>;
impl Readable for TaggedFields {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        let mut result = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            let item = TaggedField::read(buffer)?;
            result.push(item);
        }

        Ok(result)
    }
}
impl Writable for TaggedFields {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.len() as u32;
        let mut data = self.iter().collect::<Vec<_>>();
        data.sort_by_key(|tf| tf.key);

        write_unsigned_varint(sz, buffer);
        for tf in data {
            tf.write(buffer);
        }
    }
}
