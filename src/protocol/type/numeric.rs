use bytes::{Buf, BufMut};

use crate::protocol::{self, Readable, ReadableResult, Writable};

impl Readable for bool {
    fn read(buffer: &mut impl Buf) -> bool {
        u8::read(buffer) != 0
    }
}
impl Writable for bool {
    fn write(&self, buffer: &mut impl BufMut) {
        (if *self { 1u8 } else { 0u8 }).write(buffer);
    }
}

impl Readable for i8 {
    fn read(buffer: &mut impl Buf) -> i8 {
        buffer.get_i8()
    }
}
impl Writable for i8 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i8(*self);
    }
}

impl Readable for u8 {
    fn read(buffer: &mut impl Buf) -> u8 {
        buffer.get_u8()
    }
}
impl Writable for u8 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u8(*self);
    }
}

impl Readable for i16 {
    fn read(buffer: &mut impl Buf) -> i16 {
        buffer.get_i16()
    }
}
impl Writable for i16 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i16(*self);
    }
}

impl Readable for u16 {
    fn read(buffer: &mut impl Buf) -> u16 {
        buffer.get_u16()
    }
}
impl Writable for u16 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u16(*self);
    }
}

impl Readable for i32 {
    fn read(buffer: &mut impl Buf) -> i32 {
        buffer.get_i32()
    }
}
impl Writable for i32 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i32(*self);
    }
}

impl Readable for u32 {
    fn read(buffer: &mut impl Buf) -> u32 {
        buffer.get_u32()
    }
}
impl Writable for u32 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u32(*self);
    }
}

impl Readable for i64 {
    fn read(buffer: &mut impl Buf) -> i64 {
        buffer.get_i64()
    }
}
impl Writable for i64 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i64(*self);
    }
}

pub(crate) fn write_unsigned_varint(buffer: &mut impl BufMut, value: u32) {
    if (value & (0xFFFFFFFF << 7)) == 0 {
        (value as u8).write(buffer);
    } else {
        ((value & 0x7f) as u8 | 0x80).write(buffer);
        if (value & (0xFFFFFFFF << 14)) == 0 {
            ((value >> 7) as u8).write(buffer);
        } else {
            (((value >> 7) & 0x7F | 0x80) as u8).write(buffer);
            if (value & (0xFFFFFFFF << 21)) == 0 {
                ((value >> 14) as u8).write(buffer);
            } else {
                (((value >> 14) & 0x7F | 0x80) as u8).write(buffer);
                if (value & (0xFFFFFFFF << 28)) == 0 {
                    ((value >> 21) as u8).write(buffer);
                } else {
                    (((value >> 21) & 0x7F | 0x80) as u8).write(buffer);
                    (((value >> 28) & 0xFF) as u8).write(buffer);
                }
            }
        }
    }
}

pub(crate) fn read_unsigned_varint(buffer: &mut impl Buf) -> Result<u32, protocol::Error> {
    let tmp = i8::read(buffer);
    if tmp >= 0 {
        Ok((tmp as u8) as u32)
    } else {
        let mut result: u32 = (tmp as u8 & 0x7F) as u32;
        let tmp = i8::read(buffer);
        if tmp >= 0 {
            result |= ((tmp as u8) as u32) << 7;
        } else {
            result |= ((tmp as u8 & 0x7F) as u32) << 7;
            let tmp = i8::read(buffer);
            if tmp >= 0 {
                result |= ((tmp as u8) as u32) << 14;
            } else {
                result |= ((tmp as u8 & 0x7F) as u32) << 14;
                let tmp = i8::read(buffer);
                if tmp >= 0 {
                    result |= ((tmp as u8) as u32) << 21;
                } else {
                    result |= ((tmp as u8 & 0x7f) as u32) << 21;
                    let tmp = i8::read(buffer);
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

pub(crate) fn write_unsigned_varlong(buffer: &mut impl BufMut, mut value: u64) {
    while value & 0xFFFFFFFFFFFFFF80 != 0 {
        (value as u8 & 0x7F | 0x80).write(buffer);
        value >>= 7;
    }
    (value as u8).write(buffer);
}

pub(crate) fn read_unsigned_varlong(buffer: &mut impl Buf) -> Result<u64, protocol::Error> {
    let mut value: u64 = 0;
    let mut i = 0;
    loop {
        let b = i8::read(buffer);
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

    pub(crate) fn read_result_inner(buffer: &mut impl Buf) -> Result<i32, protocol::Error> {
        let value = read_unsigned_varint(buffer)?;
        Ok((value as i32 >> 1) ^ -(value as i32 & 1))
    }

    pub(crate) fn write_inner(buffer: &mut impl BufMut, value: i32) {
        let value = ((value << 1) ^ (value >> 31)) as u32;
        write_unsigned_varint(buffer, value);
    }
}
impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        Self(value)
    }
}
impl ReadableResult for VarInt {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        Self::read_result_inner(buffer).map(Self)
    }
}
impl Writable for VarInt {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write_inner(buffer, self.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VarLong(i64);
impl VarLong {
    pub fn value(&self) -> i64 {
        self.0
    }

    pub(crate) fn read_result_inner(buffer: &mut impl Buf) -> Result<i64, protocol::Error> {
        let value = read_unsigned_varlong(buffer)?;
        Ok((value as i64 >> 1) ^ -(value as i64 & 1))
    }

    pub(crate) fn write_inner(buffer: &mut impl BufMut, value: i64) {
        let value = ((value << 1) ^ (value >> 63)) as u64;
        write_unsigned_varlong(buffer, value);
    }
}
impl From<i64> for VarLong {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl ReadableResult for VarLong {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        Self::read_result_inner(buffer).map(Self)
    }
}
impl Writable for VarLong {
    fn write(&self, buffer: &mut impl BufMut) {
        Self::write_inner(buffer, self.0);
    }
}

impl Readable for f64 {
    fn read(buffer: &mut impl Buf) -> f64 {
        buffer.get_f64()
    }
}
impl Writable for f64 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_f64(*self);
    }
}
