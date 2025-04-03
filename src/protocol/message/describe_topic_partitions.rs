#![allow(clippy::too_many_arguments)]
use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes};
use uuid::Uuid;

use crate::{
    model,
    protocol::{
        self,
        r#type::{CompactArray, CompactKafkaString, NullableRecord, TaggedFields},
        Readable, ReadableVersion, Writable,
    },
};

use super::topic_by_name;

pub(crate) const API_KEY: i16 = 75;

#[derive(Debug, Clone)]
pub struct Request {
    topics: Vec<TopicRequest>,
    response_partition_limit: i32,
    cursor: Option<Cursor>,
}
impl Request {
    pub fn v0(
        topics: Vec<TopicRequest>,
        response_partition_limit: i32,
        cursor: Option<Cursor>,
    ) -> Self {
        Request {
            topics,
            response_partition_limit,
            cursor,
        }
    }

    pub fn topics(&self) -> &[TopicRequest] {
        &self.topics
    }

    pub fn response_partition_limit(&self) -> i32 {
        self.response_partition_limit
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }
}
impl ReadableVersion for Request {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let topics = CompactArray::<TopicRequest>::read_version_inner(buffer, version)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field topic was serialized as null"),
        )?;
        let response_partition_limit = i32::read(buffer);
        let cursor = NullableRecord::read_version(buffer, version)?;
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        Ok(Request {
            topics,
            response_partition_limit,
            cursor,
        })
    }
}
impl Writable for Request {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactArray::write_inner(buffer, Some(self.topics()));
        self.response_partition_limit.write(buffer);
        NullableRecord::write_inner(buffer, self.cursor());
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct TopicRequest {
    name: Bytes,
}
impl TopicRequest {
    pub fn v0(name: &str) -> Self {
        TopicRequest {
            name: Bytes::copy_from_slice(name.as_bytes()),
        }
    }

    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }
}
impl ReadableVersion for TopicRequest {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field name was serialized as null"),
        )?;
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        Ok(TopicRequest { name })
    }
}
impl Writable for TopicRequest {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactKafkaString::write_inner(buffer, Some(self.name()));
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct Cursor {
    topic_name: Bytes,
    partition_index: i32,
}
impl Cursor {
    pub fn v0(topic_name: &str, partition_index: i32) -> Self {
        Cursor {
            topic_name: Bytes::copy_from_slice(topic_name.as_bytes()),
            partition_index,
        }
    }

    pub fn topic_name(&self) -> &[u8] {
        self.topic_name.as_ref()
    }

    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }
}
impl ReadableVersion for Cursor {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let topic_name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field topicName was serialized as null"),
        )?;
        let partition_index = i32::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        Ok(Cursor {
            topic_name,
            partition_index,
        })
    }
}
impl Writable for Cursor {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactKafkaString::write_inner(buffer, Some(self.topic_name()));
        self.partition_index.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    throttle_time_ms: i32,
    topics: Vec<DescribeTopicPartitionsResponseTopic>,
    next_cursor: Option<Cursor>,
}
impl Response {
    pub fn v0(
        throttle_time_ms: i32,
        topics: Vec<DescribeTopicPartitionsResponseTopic>,
        next_cursor: Option<Cursor>,
    ) -> Self {
        Response {
            throttle_time_ms,
            topics,
            next_cursor,
        }
    }

    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
    }

    pub fn topics(&self) -> &[DescribeTopicPartitionsResponseTopic] {
        self.topics.as_ref()
    }

    pub fn next_cursor(&self) -> Option<&Cursor> {
        self.next_cursor.as_ref()
    }
}
impl ReadableVersion for Response {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let throttle_time_ms = i32::read(buffer);
        let topics = CompactArray::read_version_inner(buffer, version)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field topics was serialized as null"),
        )?;
        let next_cursor = NullableRecord::read_version(buffer, version)?;
        Ok(Response {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}
impl Writable for Response {
    fn write(&self, buffer: &mut impl BufMut) {
        self.throttle_time_ms.write(buffer);
        CompactArray::write_inner(buffer, Some(self.topics()));
        NullableRecord::write_inner(buffer, self.next_cursor());
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsResponseTopic {
    error_code: i16,
    name: Option<Bytes>,
    topic_id: Uuid,
    is_internal: bool,
    partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    topic_authorized_operations: i32,
}
impl DescribeTopicPartitionsResponseTopic {
    pub fn v0(
        error_code: i16,
        name: Option<&str>,
        topic_id: Uuid,
        is_internal: bool,
        partitions: Vec<DescribeTopicPartitionsResponsePartition>,
        topic_authorized_operations: i32,
    ) -> Self {
        Self {
            error_code,
            name: name.map(|n| Bytes::copy_from_slice(n.as_bytes())),
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        }
    }

    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    pub fn name(&self) -> Option<&[u8]> {
        self.name.as_deref()
    }

    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    pub fn partitions(&self) -> &[DescribeTopicPartitionsResponsePartition] {
        self.partitions.as_ref()
    }

    pub fn topic_authorized_operations(&self) -> i32 {
        self.topic_authorized_operations
    }
}
impl ReadableVersion for DescribeTopicPartitionsResponseTopic {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let error_code = i16::read(buffer);
        let name = CompactKafkaString::read_result_inner(buffer)?;
        let topic_id = Uuid::read(buffer);
        let is_internal = bool::read(buffer);
        let partitions = CompactArray::read_version_inner(buffer, version)?.ok_or(
            protocol::Error::IllegalArgument(
                "non-nullable field partitions was serialized as null",
            ),
        )?;
        let topic_authorized_operations = i32::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        Ok(Self {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        })
    }
}
impl Writable for DescribeTopicPartitionsResponseTopic {
    fn write(&self, buffer: &mut impl BufMut) {
        self.error_code.write(buffer);
        CompactKafkaString::write_inner(buffer, self.name());
        self.topic_id.write(buffer);
        self.is_internal.write(buffer);
        CompactArray::write_inner(buffer, Some(self.partitions()));
        self.topic_authorized_operations.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
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
impl DescribeTopicPartitionsResponsePartition {
    pub fn v0(
        error_code: i16,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
        replica_nodes: Vec<i32>,
        isr_nodes: Vec<i32>,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_elr: Option<Vec<i32>>,
        offline_replicas: Vec<i32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        }
    }

    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    pub fn leader_id(&self) -> i32 {
        self.leader_id
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    pub fn replica_nodes(&self) -> &[i32] {
        self.replica_nodes.as_ref()
    }

    pub fn isr_nodes(&self) -> &[i32] {
        self.isr_nodes.as_ref()
    }

    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    pub fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }

    pub fn offline_replicas(&self) -> &[i32] {
        self.offline_replicas.as_ref()
    }
}
impl ReadableVersion for DescribeTopicPartitionsResponsePartition {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version != 0 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let error_code = i16::read(buffer);
        let partition_index = i32::read(buffer);
        let leader_id = i32::read(buffer);
        let leader_epoch = i32::read(buffer);
        let replica_nodes =
            CompactArray::<i32>::read_inner(buffer)?.ok_or(protocol::Error::IllegalArgument(
                "non-nullable field replicaNodes was serialized as null",
            ))?;
        let isr_nodes = CompactArray::<i32>::read_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field isrNodes was serialized as null"),
        )?;
        let eligible_leader_replicas = CompactArray::<i32>::read_inner(buffer)?;
        let last_known_elr = CompactArray::<i32>::read_inner(buffer)?;
        let offline_replicas =
            CompactArray::<i32>::read_inner(buffer)?.ok_or(protocol::Error::IllegalArgument(
                "non-nullable field offlineReplicas was serialized as null",
            ))?;
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        })
    }
}
impl Writable for DescribeTopicPartitionsResponsePartition {
    fn write(&self, buffer: &mut impl BufMut) {
        self.error_code.write(buffer);
        self.partition_index.write(buffer);
        self.leader_id.write(buffer);
        self.leader_epoch.write(buffer);
        CompactArray::write_inner(buffer, Some(self.replica_nodes()));
        CompactArray::write_inner(buffer, Some(self.isr_nodes()));
        CompactArray::write_inner(buffer, self.eligible_leader_replicas());
        CompactArray::write_inner(buffer, self.last_known_elr());
        CompactArray::write_inner(buffer, Some(self.offline_replicas()));
        TaggedFields::write_empty(buffer);
    }
}

pub fn process_request(
    request: Request,
    metadata: &HashMap<Uuid, model::Topic>,
) -> Result<Response, protocol::Error> {
    let topics = {
        let mut result = Vec::new();
        for tr in request.topics() {
            let topic_name = std::str::from_utf8(tr.name())?;
            let response = if let Some(topic) = topic_by_name(metadata, topic_name) {
                DescribeTopicPartitionsResponseTopic {
                    error_code: 0,
                    name: Some(tr.name.clone()),
                    topic_id: topic.id(),
                    is_internal: false,
                    partitions: {
                        let mut result = vec![];
                        for (index, partition) in topic.partitions().iter().enumerate() {
                            result.push(DescribeTopicPartitionsResponsePartition {
                                error_code: 0,
                                partition_index: index as i32,
                                leader_id: partition.leader(),
                                leader_epoch: partition.leader_epoch(),
                                replica_nodes: partition.replicas().to_vec(),
                                isr_nodes: partition.isr().to_vec(),
                                eligible_leader_replicas: partition
                                    .eligible_leader_replicas()
                                    .map(|v| v.to_vec()),
                                last_known_elr: partition.last_known_elr().map(|v| v.to_vec()),
                                offline_replicas: vec![],
                            });
                        }
                        result
                    },
                    topic_authorized_operations: 0xdf8,
                }
            } else {
                DescribeTopicPartitionsResponseTopic {
                    error_code: 3,
                    name: Some(tr.name.clone()),
                    topic_id: Uuid::nil(),
                    is_internal: false,
                    partitions: vec![],
                    topic_authorized_operations: 0xdf8,
                }
            };

            result.push(response);
        }
        result
    };
    Ok(Response {
        throttle_time_ms: 0,
        topics,
        next_cursor: None,
    })
}
