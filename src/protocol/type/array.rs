use bytes::{Buf, BufMut};

use crate::protocol::{self, r#type::read_unsigned_varint, Readable, Writable};

use super::write_unsigned_varint;

#[derive(Debug)]
pub struct Array<T>(Vec<T>);
impl<T> Array<T> {
    pub fn new(value: Vec<T>) -> Self {
        Self(value)
    }

    pub fn data(&self) -> &Vec<T> {
        &self.0
    }

    pub(crate) fn write(buffer: &mut impl BufMut, value: &[T])
    where
        T: Writable,
    {
        let sz = value.len() as i32;
        buffer.put_i32(sz);
        for t in value {
            t.write(buffer);
        }
    }
}
impl<T> Readable for Array<T>
where
    T: Readable,
{
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self(vec![]));
        }

        let sz = sz as usize;
        let mut value = Vec::with_capacity(sz);
        for _ in 0..sz {
            let item = T::read(buffer)?;
            value.push(item);
        }
        Ok(Self(value))
    }
}
impl<T> Writable for Array<T>
where
    T: Writable,
{
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, &self.0);
    }
}

#[derive(Debug)]
pub struct CompactArray<T>(Option<Vec<T>>);
impl<T> CompactArray<T> {
    pub fn value(&self) -> Option<&Vec<T>> {
        self.0.as_ref()
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, data: &[T])
    where
        T: Writable,
    {
        let sz = data.len();
        write_unsigned_varint(sz as u32 + 1, buffer);

        for t in data {
            t.write(buffer);
        }
    }

    #[inline(always)]
    pub(crate) fn write_empty(buffer: &mut impl BufMut) {
        buffer.put_u8(0);
    }
}
impl<T> From<Option<Vec<T>>> for CompactArray<T> {
    fn from(value: Option<Vec<T>>) -> Self {
        Self(value)
    }
}
impl<T> From<Vec<T>> for CompactArray<T> {
    fn from(value: Vec<T>) -> Self {
        Self(Some(value))
    }
}
impl<T> Readable for CompactArray<T>
where
    T: Readable,
{
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Self(None));
        }

        let sz = sz as usize - 1;
        let mut value = Vec::with_capacity(sz);
        for _ in 0..sz {
            let item = T::read(buffer)?;
            value.push(item);
        }
        Ok(Self(Some(value)))
    }
}
impl<T> Writable for CompactArray<T>
where
    T: Writable,
{
    fn write(&self, buffer: &mut impl BufMut) {
        if let Some(value) = &self.0 {
            Self::write(buffer, value);
        } else {
            Self::write_empty(buffer);
        }
    }
}
