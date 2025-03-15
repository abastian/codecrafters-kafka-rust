use bytes::{Buf, BufMut};

use crate::protocol::{self, Readable, Writable};

pub struct Boolean(pub bool);
impl Readable for Boolean {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u8() != 0;
        Ok(Self(value))
    }
}
impl Writable for Boolean {
    fn write(&self, buffer: &mut impl BufMut) {
        let value: u8 = if self.0 { 1 } else { 0 };
        buffer.put_u8(value);
    }
}

pub struct Int8(pub i8);
impl Readable for Int8 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i8();
        Ok(Self(value))
    }
}
impl Writable for Int8 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i8(self.0)
    }
}

pub struct Int16(pub i16);
impl Readable for Int16 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i16();
        Ok(Self(value))
    }
}
impl Writable for Int16 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i16(self.0)
    }
}

pub struct Int32(pub i32);
impl Readable for Int32 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i32();
        Ok(Self(value))
    }
}
impl Writable for Int32 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i32(self.0)
    }
}

pub struct Int64(pub i64);
impl Readable for Int64 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_i64();
        Ok(Self(value))
    }
}
impl Writable for Int64 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i64(self.0)
    }
}

pub struct UInt16(pub u16);
impl Readable for UInt16 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u16();
        Ok(Self(value))
    }
}
impl Writable for UInt16 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u16(self.0)
    }
}

pub struct UInt32(pub u32);
impl Readable for UInt32 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_u32();
        Ok(Self(value))
    }
}
impl Writable for UInt32 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_u32(self.0)
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

pub struct VarInt(pub i32);
impl Readable for VarInt {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = read_unsigned_varint(buffer)?;
        Ok(Self((value as i32 >> 1) ^ -(value as i32 & 1)))
    }
}
impl Writable for VarInt {
    fn write(&self, buffer: &mut impl BufMut) {
        let value = ((self.0 << 1) ^ (self.0 >> 31)) as u32;
        write_unsigned_varint(value, buffer);
    }
}

pub struct VarLong(pub i64);
impl Readable for VarLong {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = read_unsigned_varlong(buffer)?;
        Ok(Self((value as i64 >> 1) ^ -(value as i64 & 1)))
    }
}
impl Writable for VarLong {
    fn write(&self, buffer: &mut impl BufMut) {
        let value = ((self.0 << 1) ^ (self.0 >> 63)) as u64;
        write_unsigned_varlong(value, buffer);
    }
}

pub struct Float64(pub f64);
impl Readable for Float64 {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let value = buffer.get_f64();
        Ok(Self(value))
    }
}
impl Writable for Float64 {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_f64(self.0);
    }
}
