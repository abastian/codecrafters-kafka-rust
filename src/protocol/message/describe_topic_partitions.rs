use bytes::{Buf, BufMut};

use crate::protocol::{
    self,
    r#type::{
        Boolean, CompactArray, CompactKafkaString, CompactNullableKafkaString, Int16, Int32,
        KafkaUuid, TaggedFields,
    },
    Readable, Writable,
};

pub struct V0Request {
    topics: Vec<TopicRequest>,
    response_partition_limit: i32,
    cursor: Option<Cursor>,
}
impl Readable for V0Request {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let topics = CompactArray::<TopicRequest>::read(buffer)?.data.ok_or(
            protocol::Error::IllegalArgument("non-nullable field topics was serialized as null"),
        )?;
        let response_partition_limit = Int32::read(buffer)?;
        let cursor = {
            let is_exists = buffer.get_i8() >= 0;
            if is_exists {
                Some(Cursor::read(buffer)?)
            } else {
                None
            }
        };
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(V0Request {
            topics,
            response_partition_limit: response_partition_limit.0,
            cursor,
        })
    }
}

pub struct TopicRequest {
    name: String,
}
impl Readable for TopicRequest {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let name = std::str::from_utf8(CompactKafkaString::read(buffer)?.0.as_ref())?.to_owned();
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(TopicRequest { name })
    }
}

pub struct Cursor {
    topic_name: String,
    partition_index: i32,
}
impl Readable for Cursor {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let topic_name =
            std::str::from_utf8(CompactKafkaString::read(buffer)?.0.as_ref())?.to_owned();
        let partition_index = Int32::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(Cursor {
            topic_name,
            partition_index: partition_index.0,
        })
    }
}
impl Writable for &Cursor {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactKafkaString(self.topic_name.clone()).write(buffer);
        Int32(self.partition_index).write(buffer);
        buffer.put_u8(0); // empty _tagged_fields
    }
}

pub struct V0Response {
    correlation_id: i32,
    throttle_time_ms: i32,
    topics: Vec<DescribeTopicPartitionsResponseTopic>,
    next_cursor: Option<Cursor>,
}
impl Writable for V0Response {
    fn write(&self, buffer: &mut impl BufMut) {
        buffer.put_i32(self.correlation_id);
        buffer.put_u8(0); // empty _tagged_fields
        buffer.put_i32(self.throttle_time_ms);
        CompactArray {
            data: Some(self.topics.iter().collect()),
        }
        .write(buffer);
        if let Some(cursor) = &self.next_cursor {
            buffer.put_u8(0);
            cursor.write(buffer);
        } else {
            buffer.put_i8(-1);
        }
        buffer.put_u8(0); // empty _tagged_fields
    }
}

pub struct DescribeTopicPartitionsResponseTopic {
    error_code: i16,
    name: Option<String>,
    topic_id: uuid::Uuid,
    is_internal: bool,
    partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    topic_authorized_operations: i32,
}
impl Writable for &DescribeTopicPartitionsResponseTopic {
    fn write(&self, buffer: &mut impl BufMut) {
        Int16(self.error_code).write(buffer);
        CompactNullableKafkaString(self.name.clone()).write(buffer);
        KafkaUuid(self.topic_id).write(buffer);
        Boolean(self.is_internal).write(buffer);
        CompactArray {
            data: Some(self.partitions.iter().collect()),
        }
        .write(buffer);
        Int32(self.topic_authorized_operations).write(buffer);
        buffer.put_u8(0); // empty _tagged_fields
    }
}

pub struct DescribeTopicPartitionsResponsePartition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
}
impl Writable for &DescribeTopicPartitionsResponsePartition {
    fn write(&self, buffer: &mut impl BufMut) {
        Int16(self.error_code).write(buffer);
        Int32(self.partition_index).write(buffer);
        Int32(self.leader_id).write(buffer);
        Int32(self.leader_epoch).write(buffer);
        CompactArray {
            data: Some(self.replica_nodes.iter().map(|i| Int32(*i)).collect()),
        }
        .write(buffer);
        CompactArray {
            data: Some(self.isr_nodes.iter().map(|i| Int32(*i)).collect()),
        }
        .write(buffer);
        CompactArray {
            data: self
                .eligible_leader_replicas
                .as_ref()
                .map(|v| v.iter().map(|i| Int32(*i)).collect()),
        }
        .write(buffer);
        CompactArray {
            data: self
                .last_known_elr
                .as_ref()
                .map(|v| v.iter().map(|i| Int32(*i)).collect()),
        }
        .write(buffer);
        CompactArray {
            data: Some(self.offline_replicas.iter().map(|i| Int32(*i)).collect()),
        }
        .write(buffer);
        buffer.put_u8(0); // empty _tagged_fields
    }
}

pub enum Request {
    V0(V0Request),
}

pub enum Response {
    V0(V0Response),
}
impl Writable for Response {
    fn write(&self, buffer: &mut impl BufMut) {
        match self {
            Response::V0(resp) => resp.write(buffer),
        }
    }
}

pub fn process_request(buffer: &mut impl Buf, correlation_id: i32, version: i16) -> Response {
    match version {
        0 => match V0Request::read(buffer) {
            Err(_) => todo!(),
            Ok(req) => {
                let topics = req
                    .topics
                    .iter()
                    .map(|tr| DescribeTopicPartitionsResponseTopic {
                        error_code: 3,
                        name: Some(tr.name.clone()),
                        topic_id: uuid::Uuid::nil(),
                        is_internal: false,
                        partitions: vec![],
                        topic_authorized_operations: 0xdf8,
                    })
                    .collect();
                Response::V0(V0Response {
                    correlation_id,
                    throttle_time_ms: 0,
                    topics,
                    next_cursor: None,
                })
            }
        },
        _ => todo!(),
    }
}
