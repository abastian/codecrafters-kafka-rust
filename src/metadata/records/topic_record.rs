use bytes::{Buf, Bytes};
use uuid::Uuid;

use crate::protocol::{
    self,
    r#type::{CompactKafkaString, TaggedFields},
    Readable, ReadableVersion, Writable,
};

pub(crate) const API_KEY: i16 = 2;

#[derive(Debug, Clone)]
pub struct TopicRecord {
    name: Bytes,
    topic_id: Uuid,
}
impl TopicRecord {
    pub fn new(name: &str, topic_id: Uuid) -> Self {
        TopicRecord {
            name: Bytes::copy_from_slice(name.as_bytes()),
            topic_id,
        }
    }

    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }
}
impl ReadableVersion for TopicRecord {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field name was serialized as null"),
        )?;
        let topic_id = Uuid::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        Ok(TopicRecord { name, topic_id })
    }
}
impl Writable for TopicRecord {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        CompactKafkaString::write_inner(buffer, Some(self.name()));
        self.topic_id.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}
