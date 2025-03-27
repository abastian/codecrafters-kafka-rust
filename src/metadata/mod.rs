use bytes::{Buf, BufMut, Bytes, BytesMut};

pub mod records;

use crate::protocol::{
    self,
    r#type::{VarInt, VarLong},
    Readable, ReadableResult, Writable,
};

#[derive(Debug, Clone)]
pub struct Value {}

#[derive(Debug, Clone)]
pub struct Header {
    key: Bytes,
    value: Bytes,
}
impl Header {
    pub fn new(key: &str, value: &str) -> Self {
        Self {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }
}
impl ReadableResult for Header {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let key_length = VarInt::read_result(buffer)?.value() as usize;
        let key = buffer.copy_to_bytes(key_length);
        let value_length = VarInt::read_result(buffer)?.value() as usize;
        let value = buffer.copy_to_bytes(value_length);
        Ok(Self { key, value })
    }
}
impl Writable for Header {
    fn write(&self, buffer: &mut impl BufMut) {
        VarInt::write_inner(buffer, self.key.len() as i32);
        buffer.put_slice(&self.key);
        VarInt::write_inner(buffer, self.value.len() as i32);
        buffer.put_slice(&self.value);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ControlRecord {
    version: i16,
    r#type: i16,
}
impl ControlRecord {
    pub fn new(version: i16, r#type: i16) -> Self {
        Self { version, r#type }
    }

    pub fn version(&self) -> i16 {
        self.version
    }

    pub fn r#type(&self) -> i16 {
        self.r#type
    }
}
impl Readable for ControlRecord {
    fn read(buffer: &mut impl Buf) -> Self {
        let version = i16::read(buffer);
        let r#type = i16::read(buffer);
        Self { version, r#type }
    }
}
impl Writable for ControlRecord {
    fn write(&self, buffer: &mut impl BufMut) {
        self.version.write(buffer);
        self.r#type.write(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct ValueData {
    frame_version: u8,
    r#type: u8,
    version: u8,
    data: Bytes,
}
impl ValueData {
    pub fn new(frame_version: u8, r#type: u8, version: u8, data: Bytes) -> Self {
        Self {
            frame_version,
            r#type,
            version,
            data,
        }
    }

    pub fn frame_version(&self) -> u8 {
        self.frame_version
    }

    pub fn r#type(&self) -> u8 {
        self.r#type
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}
impl Readable for ValueData {
    fn read(buffer: &mut impl Buf) -> Self {
        let frame_version = u8::read(buffer);
        let r#type = u8::read(buffer);
        let version = u8::read(buffer);
        let data = Bytes::copy_from_slice({
            let len = buffer.remaining();
            &buffer.copy_to_bytes(len)
        });
        Self {
            frame_version,
            r#type,
            version,
            data,
        }
    }
}
impl Writable for ValueData {
    fn write(&self, buffer: &mut impl BufMut) {
        self.frame_version.write(buffer);
        self.r#type.write(buffer);
        self.version.write(buffer);
        buffer.put_slice(&self.data);
    }
}

#[derive(Debug, Clone)]
pub struct ValueRecord {
    attributes: u8,
    timestamp_delta: i64,
    offset_delta: i32,
    key: Option<Bytes>,
    value: ValueData,
    headers: Vec<Header>,
}
impl ValueRecord {
    pub fn new(
        attributes: u8,
        timestamp_delta: i64,
        offset_delta: i32,
        key: Option<&str>,
        value: ValueData,
        headers: Vec<Header>,
    ) -> Self {
        Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            value,
            headers,
        }
    }

    pub fn attributes(&self) -> u8 {
        self.attributes
    }

    pub fn timestamp_delta(&self) -> i64 {
        self.timestamp_delta
    }

    pub fn offset_delta(&self) -> i32 {
        self.offset_delta
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value(&self) -> &ValueData {
        &self.value
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}
impl ReadableResult for ValueRecord {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let length = VarInt::read_result_inner(buffer)? as usize;
        let mut inner_buffer = buffer.copy_to_bytes(length);

        let attributes = u8::read(&mut inner_buffer);
        let timestamp_delta = VarLong::read_result_inner(&mut inner_buffer)?;
        let offset_delta = VarInt::read_result_inner(&mut inner_buffer)?;
        let key = {
            let key_length = VarInt::read_result_inner(&mut inner_buffer)?;
            if key_length < 0 {
                None
            } else {
                Some(inner_buffer.copy_to_bytes(key_length as usize))
            }
        };
        let value = {
            let value_length = VarInt::read_result_inner(&mut inner_buffer)? as usize;
            let mut inner_buffer = inner_buffer.copy_to_bytes(value_length);
            ValueData::read(&mut inner_buffer)
        };
        let headers = {
            let length = VarInt::read_result_inner(&mut inner_buffer)?;
            let mut result = Vec::with_capacity(length as usize);
            for _ in 0..length {
                result.push(Header::read_result(&mut inner_buffer)?);
            }
            result
        };
        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}
impl Writable for ValueRecord {
    fn write(&self, buffer: &mut impl BufMut) {
        let mut inner_buffer = BytesMut::with_capacity(64);
        self.attributes.write(&mut inner_buffer);
        self.timestamp_delta.write(&mut inner_buffer);
        self.offset_delta.write(&mut inner_buffer);
        {
            if let Some(key) = &self.key {
                VarInt::write_inner(&mut inner_buffer, key.len() as i32);
                inner_buffer.put_slice(key);
            } else {
                1u8.write(&mut inner_buffer);
            }
        }
        VarInt::write_inner(&mut inner_buffer, self.value.data.len() as i32 + 3);
        self.value.write(&mut inner_buffer);
        VarInt::write_inner(&mut inner_buffer, self.headers.len() as i32);
        for header in &self.headers {
            header.write(&mut inner_buffer);
        }

        let inner_buffer = inner_buffer.freeze();
        let record_length = inner_buffer.len();
        VarInt::write_inner(buffer, record_length as i32);
        buffer.put_slice(&inner_buffer);
    }
}

#[derive(Debug, Clone)]
pub enum Record {
    Value(ValueRecord),
    Control(ControlRecord),
}
impl Writable for Record {
    fn write(&self, buffer: &mut impl BufMut) {
        match self {
            Record::Value(record) => record.write(buffer),
            Record::Control(record) => record.write(buffer),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    base_offset: i64,
    partition_leader_epoch: i32,
    magic_byte: u8,
    attributes: u16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}
impl RecordBatch {
    pub fn new(
        base_offset: i64,
        partition_leader_epoch: i32,
        magic_byte: u8,
        attributes: u16,
        last_offset_delta: i32,
        base_timestamp: i64,
        max_timestamp: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: Vec<Record>,
    ) -> Self {
        Self {
            base_offset,
            partition_leader_epoch,
            magic_byte,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn partition_leader_epoch(&self) -> i32 {
        self.partition_leader_epoch
    }

    pub fn magic_byte(&self) -> u8 {
        self.magic_byte
    }

    pub fn attributes(&self) -> u16 {
        self.attributes
    }

    pub fn is_control_batch(&self) -> bool {
        self.attributes & 0x10 != 0
    }

    pub fn last_offset_delta(&self) -> i32 {
        self.last_offset_delta
    }

    pub fn base_timestamp(&self) -> i64 {
        self.base_timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    pub fn base_sequence(&self) -> i32 {
        self.base_sequence
    }

    pub fn records(&self) -> &[Record] {
        self.records.as_ref()
    }
}
impl ReadableResult for RecordBatch {
    fn read_result(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        if buffer.remaining() < 12 {
            return Err(protocol::Error::BufferUnderflow);
        }
        let base_offset = i64::read(buffer);
        let mut inner_buffer = {
            let batch_length = u32::read(buffer) as usize;
            if buffer.remaining() < batch_length {
                return Err(protocol::Error::BufferUnderflow);
            }
            buffer.copy_to_bytes(batch_length)
        };

        let partition_leader_epoch = i32::read(&mut inner_buffer);
        let magic_byte = u8::read(&mut inner_buffer);
        let crc_read = u32::read(&mut inner_buffer);
        let crc_check = crc32c::crc32c(&inner_buffer);
        if crc_read != crc_check {
            return Err(protocol::Error::IllegalArgument("crc mismatch"));
        }

        let attributes = u16::read(&mut inner_buffer);
        let last_offset_delta = i32::read(&mut inner_buffer);
        let base_timestamp = i64::read(&mut inner_buffer);
        let max_timestamp = i64::read(&mut inner_buffer);
        let producer_id = i64::read(&mut inner_buffer);
        let producer_epoch = i16::read(&mut inner_buffer);
        let base_sequence = i32::read(&mut inner_buffer);
        let records = {
            let records_length = i32::read(&mut inner_buffer) as usize;
            if attributes & 0x10 != 0 {
                if records_length != 1 {
                    return Err(protocol::Error::IllegalArgument(
                        "invalid records length for ControlBatch",
                    ));
                }
                vec![Record::Control(ControlRecord::read(&mut inner_buffer))]
            } else {
                let mut records = Vec::with_capacity(records_length);
                for _ in 0..records_length {
                    records.push(Record::Value(ValueRecord::read_result(&mut inner_buffer)?));
                }
                records
            }
        };
        Ok(Self {
            base_offset,
            partition_leader_epoch,
            magic_byte,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}
impl Writable for RecordBatch {
    fn write(&self, buffer: &mut impl BufMut) {
        let mut inner_buffer = BytesMut::with_capacity(64);
        self.attributes.write(&mut inner_buffer);
        self.last_offset_delta.write(&mut inner_buffer);
        self.base_timestamp.write(&mut inner_buffer);
        self.max_timestamp.write(&mut inner_buffer);
        self.producer_id.write(&mut inner_buffer);
        self.producer_epoch.write(&mut inner_buffer);
        self.base_sequence.write(&mut inner_buffer);
        {
            let records_length = self.records.len() as i32;
            VarInt::write_inner(&mut inner_buffer, records_length);
            for record in &self.records {
                record.write(&mut inner_buffer);
            }
        }

        let inner_buffer = inner_buffer.freeze();
        let crc = crc32c::crc32c(&inner_buffer);
        let batch_length = inner_buffer.len() as i32;
        self.base_offset.write(buffer);
        batch_length.write(buffer);
        self.partition_leader_epoch.write(buffer);
        self.magic_byte.write(buffer);
        crc.write(buffer);
        buffer.put_slice(&inner_buffer);
    }
}
