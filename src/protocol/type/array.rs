use bytes::{Buf, BufMut};

use crate::protocol::{self, Readable, ReadableResult, ReadableVersion, Writable};

use super::{read_unsigned_varint, write_unsigned_varint};

#[derive(Debug)]
pub struct Array<T>(Option<Vec<T>>);
impl<T> Array<T> {
    pub fn value(&self) -> Option<&[T]> {
        self.0.as_deref()
    }

    pub(crate) fn write_none<B: BufMut>(buffer: &mut B) {
        (-1i8).write(buffer);
    }
}
impl<T> Array<T>
where
    T: Readable,
{
    pub(crate) fn read_inner<B: Buf>(buffer: &mut B) -> Option<Vec<T>> {
        let sz = i32::read(buffer);
        if sz < 0 {
            return None;
        }
        if sz == 0 {
            return Some(vec![]);
        }

        let mut value = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            value.push(T::read(buffer));
        }
        Some(value)
    }
}
impl<T> Array<T>
where
    T: ReadableVersion,
{
    pub(crate) fn read_version_inner<B: Buf>(
        buffer: &mut B,
        version: i16,
    ) -> Result<Option<Vec<T>>, protocol::Error> {
        let sz = i32::read(buffer);
        if sz < 0 {
            return Ok(None);
        }
        if sz == 0 {
            return Ok(Some(vec![]));
        }

        let mut value = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            value.push(T::read_version(buffer, version)?);
        }
        Ok(Some(value))
    }
}
impl<T> Array<T>
where
    T: Writable,
{
    pub(crate) fn write_inner<B: BufMut>(buffer: &mut B, value: Option<&[T]>) {
        if let Some(value) = value {
            (value.len() as i32).write(buffer);
            for t in value {
                t.write(buffer);
            }
        } else {
            Self::write_none(buffer);
        }
    }
}
impl<T> Clone for Array<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> From<Option<Vec<T>>> for Array<T> {
    fn from(value: Option<Vec<T>>) -> Self {
        Self(value)
    }
}
impl<T> Readable for Array<T>
where
    T: Readable,
{
    fn read<B: Buf>(buffer: &mut B) -> Self {
        Self(Self::read_inner(buffer))
    }
}
impl<T> ReadableVersion for Array<T>
where
    T: ReadableVersion,
{
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        Ok(Self(Self::read_version_inner(buffer, version)?))
    }
}
impl<T> Writable for Array<T>
where
    T: Writable,
{
    fn write<B: BufMut>(&self, buffer: &mut B) {
        Self::write_inner(buffer, self.value());
    }
}

#[derive(Debug)]
pub struct CompactArray<T>(Option<Vec<T>>);
impl<T> CompactArray<T> {
    pub fn value(&self) -> Option<&[T]> {
        self.0.as_deref()
    }

    pub(crate) fn write_none<B: BufMut>(buffer: &mut B) {
        0u8.write(buffer);
    }
}
impl<T> CompactArray<T>
where
    T: Readable,
{
    pub(crate) fn read_inner<B: Buf>(buffer: &mut B) -> Result<Option<Vec<T>>, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(Some(vec![]));
        }

        let mut data = Vec::with_capacity(sz as usize - 1);
        for _ in 0..sz - 1 {
            data.push(T::read(buffer));
        }
        Ok(Some(data))
    }
}
impl<T> CompactArray<T>
where
    T: ReadableResult,
{
    pub(crate) fn read_result_inner<B: Buf>(
        buffer: &mut B,
    ) -> Result<Option<Vec<T>>, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(None);
        }
        if sz == 1 {
            return Ok(Some(vec![]));
        }

        let sz = sz as usize - 1;
        let mut data = Vec::with_capacity(sz);
        for _ in 0..sz {
            data.push(T::read_result(buffer)?);
        }
        Ok(Some(data))
    }
}
impl<T> CompactArray<T>
where
    T: ReadableVersion,
{
    pub(crate) fn read_version_inner<B: Buf>(
        buffer: &mut B,
        version: i16,
    ) -> Result<Option<Vec<T>>, protocol::Error> {
        let sz = read_unsigned_varint(buffer)?;
        if sz == 0 {
            return Ok(None);
        }
        if sz == 1 {
            return Ok(Some(vec![]));
        }

        let sz = sz as usize - 1;
        let mut data = Vec::with_capacity(sz);
        for _ in 0..sz {
            data.push(T::read_version(buffer, version)?);
        }
        Ok(Some(data))
    }
}
impl<T> CompactArray<T>
where
    T: Writable,
{
    pub(crate) fn write_inner<B: BufMut>(buffer: &mut B, value: Option<&[T]>) {
        if let Some(value) = value {
            let sz = value.len();
            write_unsigned_varint(buffer, sz as u32 + 1);

            for t in value {
                t.write(buffer);
            }
        } else {
            Self::write_none(buffer);
        }
    }
}
impl<T> Clone for CompactArray<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> From<Option<Vec<T>>> for CompactArray<T> {
    fn from(value: Option<Vec<T>>) -> Self {
        Self(value)
    }
}
impl<T> ReadableResult for CompactArray<T>
where
    T: ReadableResult,
{
    fn read_result<B: Buf>(buffer: &mut B) -> Result<Self, protocol::Error> {
        Ok(Self(Self::read_result_inner(buffer)?))
    }
}
impl<T> ReadableVersion for CompactArray<T>
where
    T: ReadableVersion,
{
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        Ok(Self(Self::read_version_inner(buffer, version)?))
    }
}
impl<T> Writable for CompactArray<T>
where
    T: Writable,
{
    fn write<B: BufMut>(&self, buffer: &mut B) {
        Self::write_inner(buffer, self.value());
    }
}
