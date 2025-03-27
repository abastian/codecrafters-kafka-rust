use bytes::{Buf, BufMut};

use crate::protocol::{self, r#type::TaggedFields, Readable, ReadableVersion, Writable};

pub struct ResponseHeader {
    version: i16,
    correlation_id: i32,
}
impl ResponseHeader {
    pub fn v0(correlation_id: i32) -> Self {
        ResponseHeader {
            version: 0,
            correlation_id,
        }
    }

    pub fn v1(correlation_id: i32) -> Self {
        ResponseHeader {
            version: 1,
            correlation_id,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }
}
impl ReadableVersion for ResponseHeader {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if (0..=1).contains(&version) {
            let correlation_id = i32::read(buffer);
            if version >= 1 {
                let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
            }
            Ok(ResponseHeader {
                version,
                correlation_id,
            })
        } else {
            Err(protocol::Error::UnsupportedVersion)
        }
    }
}
impl Writable for ResponseHeader {
    fn write(&self, buffer: &mut impl BufMut) {
        self.correlation_id.write(buffer);
        if self.version >= 1 {
            TaggedFields::write_empty(buffer);
        }
    }
}
