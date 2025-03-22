use bytes::{Buf, BufMut};

use crate::protocol::{self, Readable, Writable};

#[derive(Debug, Clone, Copy)]
pub struct Boolean(bool);
impl Boolean {
    pub fn value(&self) -> bool {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: bool) {
        let value: u8 = if value { 1 } else { 0 };
        buffer.put_u8(value);
    }
}
impl From<bool> for Boolean {
    fn from(value: bool) -> Self {
        Self(value)
    }
}
impl Readable for Boolean {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u8() != 0;
        Ok(Self(value))
    }
}
impl Writable for Boolean {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Int8(i8);
impl Int8 {
    pub fn value(&self) -> i8 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i8) {
        buffer.put_i8(value);
    }
}
impl From<i8> for Int8 {
    fn from(value: i8) -> Self {
        Self(value)
    }
}
impl Readable for Int8 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i8();
        Ok(Self(value))
    }
}
impl Writable for Int8 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Int16(i16);
impl Int16 {
    pub fn value(&self) -> i16 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i16) {
        buffer.put_i16(value);
    }
}
impl From<i16> for Int16 {
    fn from(value: i16) -> Self {
        Self(value)
    }
}
impl Readable for Int16 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i16();
        Ok(Self(value))
    }
}
impl Writable for Int16 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Int32(i32);
impl Int32 {
    pub fn value(&self) -> i32 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i32) {
        buffer.put_i32(value);
    }
}
impl From<i32> for Int32 {
    fn from(value: i32) -> Self {
        Self(value)
    }
}
impl Readable for Int32 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i32();
        Ok(Self(value))
    }
}
impl Writable for Int32 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Int64(i64);
impl Int64 {
    pub fn value(&self) -> i64 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i64) {
        buffer.put_i64(value);
    }
}
impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl Readable for Int64 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i64();
        Ok(Self(value))
    }
}
impl Writable for Int64 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UInt16(u16);
impl UInt16 {
    pub fn value(&self) -> u16 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: u16) {
        buffer.put_u16(value);
    }
}
impl From<u16> for UInt16 {
    fn from(value: u16) -> Self {
        Self(value)
    }
}
impl Readable for UInt16 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u16();
        Ok(Self(value))
    }
}
impl Writable for UInt16 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UInt32(u32);
impl UInt32 {
    pub fn value(&self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: u32) {
        buffer.put_u32(value);
    }
}
impl From<u32> for UInt32 {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Readable for UInt32 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u32();
        Ok(Self(value))
    }
}
impl Writable for UInt32 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

pub(crate) fn write_unsigned_varint(value: u32, buffer: &mut impl BufMut) {
    if (value & (0xFFFFFFFF << 7)) == 0 {
        buffer.put_u8(value as u8);
    } else {
        buffer.put_u8((value & 0x7f) as u8 | 0x80);
        if (value & (0xFFFFFFFF << 14)) == 0 {
            buffer.put_u8((value >> 7) as u8);
        } else {
            buffer.put_u8(((value >> 7) & 0x7F | 0x80) as u8);
            if (value & (0xFFFFFFFF << 21)) == 0 {
                buffer.put_u8((value >> 14) as u8);
            } else {
                buffer.put_u8(((value >> 14) & 0x7F | 0x80) as u8);
                if (value & (0xFFFFFFFF << 28)) == 0 {
                    buffer.put_u8((value >> 21) as u8);
                } else {
                    buffer.put_u8(((value >> 21) & 0x7F | 0x80) as u8);
                    buffer.put_u8(((value >> 28) & 0xFF) as u8);
                }
            }
        }
    }
}

pub(crate) fn read_unsigned_varint(buffer: &mut impl Buf) -> Result<u32, protocol::Error> {
    let tmp = buffer.get_i8();
    if tmp >= 0 {
        Ok((tmp as u8) as u32)
    } else {
        let mut result: u32 = (tmp as u8 & 0x7F) as u32;
        let tmp = buffer.get_i8();
        if tmp >= 0 {
            result |= ((tmp as u8) as u32) << 7;
        } else {
            result |= ((tmp as u8 & 0x7F) as u32) << 7;
            let tmp = buffer.get_i8();
            if tmp >= 0 {
                result |= ((tmp as u8) as u32) << 14;
            } else {
                result |= ((tmp as u8 & 0x7F) as u32) << 14;
                let tmp = buffer.get_i8();
                if tmp >= 0 {
                    result |= ((tmp as u8) as u32) << 21;
                } else {
                    result |= ((tmp as u8 & 0x7f) as u32) << 21;
                    let tmp = buffer.get_i8();
                    if tmp < 0 {
                        return Err(protocol::Error::IllegalArgument(
                            "Invalid VarInt, msb of 5th byte is set",
                        ));
                    }
                    result |= ((tmp as u8) as u32) << 28;
                }
            }
        }

        Ok(result)
    }
}

pub(crate) fn write_unsigned_varlong(mut value: u64, buffer: &mut impl BufMut) {
    while value & 0xFFFFFFFFFFFFFF80 != 0 {
        let b = value as u8 & 0x7F | 0x80;
        buffer.put_u8(b);
        value >>= 7;
    }
    buffer.put_u8(value as u8);
}

pub(crate) fn read_unsigned_varlong(buffer: &mut impl Buf) -> Result<u64, protocol::Error> {
    let mut value: u64 = 0;
    let mut i = 0;
    loop {
        let b = buffer.get_i8();
        if b >= 0 {
            value |= ((b as u8) as u64) << i;
            break Ok(value);
        }
        value |= ((b as u8 & 0x7F) as u64) << i;
        i += 7;
        if i > 63 {
            break Err(protocol::Error::IllegalArgument(
                "Invalid Varlong, msb of 10th byte is set",
            ));
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VarInt(i32);
impl VarInt {
    pub fn value(&self) -> i32 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i32) {
        let value = ((value << 1) ^ (value >> 31)) as u32;
        write_unsigned_varint(value, buffer);
    }
}
impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        Self(value)
    }
}
impl Readable for VarInt {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = read_unsigned_varint(buffer)?;
        Ok(Self((value as i32 >> 1) ^ -(value as i32 & 1)))
    }
}
impl Writable for VarInt {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VarLong(i64);
impl VarLong {
    pub fn value(&self) -> i64 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: i64) {
        let value = ((value << 1) ^ (value >> 63)) as u64;
        write_unsigned_varlong(value, buffer);
    }
}
impl From<i64> for VarLong {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl Readable for VarLong {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = read_unsigned_varlong(buffer)?;
        Ok(Self((value as i64 >> 1) ^ -(value as i64 & 1)))
    }
}
impl Writable for VarLong {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Float64(f64);
impl Float64 {
    pub fn value(&self) -> f64 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn write(buffer: &mut impl BufMut, value: f64) {
        buffer.put_f64(value);
    }
}
impl From<f64> for Float64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl Readable for Float64 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_f64();
        Ok(Self(value))
    }
}
impl Writable for Float64 {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write(buffer, self.0);
    }
}
