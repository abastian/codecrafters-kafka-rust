use crate::protocol::{self, r#type::read_unsigned_varint, Readable, Writable};

use super::write_unsigned_varint;

pub struct Array<T> {
    data: Vec<T>,
}
impl<T> Readable for Array<T>
where
    T: Readable,
{
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self { data: vec![] });
        }

        let sz = sz as usize;
        let mut data = Vec::with_capacity(sz);
        for _ in 0..sz {
            let item = T::read(buffer)?;
            data.push(item);
        }
        Ok(Self { data })
    }
}
impl<T> Writable for Array<T>
where
    T: Writable,
{
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        let sz = self.data.len() as i32;
        buffer.put_i32(sz);
        for t in &self.data {
            t.write(buffer);
        }
    }
}

pub struct CompactArray<T> {
    pub data: Option<Vec<T>>,
}
impl<T> Readable for CompactArray<T>
where
    T: Readable,
{
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self { data: None });
        }

        let sz = sz as usize - 1;
        let mut data = Vec::with_capacity(sz);
        for _ in 0..sz {
            let item = T::read(buffer)?;
            data.push(item);
        }
        Ok(Self { data: Some(data) })
    }
}
impl<T> Writable for CompactArray<T>
where
    T: Writable,
{
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        if let Some(data) = &self.data {
            let sz = data.len();
            write_unsigned_varint(sz as u32 + 1, buffer);

            for t in data {
                t.write(buffer);
            }
        } else {
            buffer.put_u8(0);
        }
    }
}
