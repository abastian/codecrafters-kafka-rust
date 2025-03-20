use bytes::{Buf, BufMut};

use crate::protocol::{
    self,
    r#type::{
        Boolean, CompactArray, CompactKafkaString, CompactNullableKafkaString, Int16, Int32,
        KafkaUuid, NullableRecord, TaggedFields,
    },
    Readable, Writable,
};

pub struct V0Request {
    topics: CompactArray<TopicRequest>,
    response_partition_limit: Int32,
    cursor: NullableRecord<Cursor>,
}
impl V0Request {
    pub fn new(
        topics: Vec<TopicRequest>,
        response_partition_limit: i32,
        cursor: Option<Cursor>,
    ) -> Self {
        V0Request {
            topics: topics.into(),
            response_partition_limit: response_partition_limit.into(),
            cursor: cursor.into(),
        }
    }
}
impl Readable for V0Request {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let topics = CompactArray::<TopicRequest>::read(buffer)?;
        if topics.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field topic was serialized as null",
            ));
        }
        let response_partition_limit = Int32::read(buffer)?;
        let cursor = NullableRecord::<Cursor>::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(V0Request {
            topics,
            response_partition_limit,
            cursor,
        })
    }
}

pub struct TopicRequest {
    name: CompactKafkaString,
}
impl Readable for TopicRequest {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let name = CompactKafkaString::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(TopicRequest { name })
    }
}

pub struct Cursor {
    topic_name: CompactKafkaString,
    partition_index: Int32,
}
impl Readable for Cursor {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let topic_name = CompactKafkaString::read(buffer)?;
        let partition_index = Int32::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(Cursor {
            topic_name,
            partition_index,
        })
    }
}
impl Writable for Cursor {
    fn write(&self, buffer: &mut impl BufMut) {
        self.topic_name.write(buffer);
        self.partition_index.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

pub struct V0Response {
    correlation_id: Int32,
    throttle_time_ms: Int32,
    topics: CompactArray<DescribeTopicPartitionsResponseTopic>,
    next_cursor: NullableRecord<Cursor>,
}
impl Readable for V0Response {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let correlation_id = Int32::read(buffer)?;
        let throttle_time_ms = Int32::read(buffer)?;
        let topics = CompactArray::read(buffer)?;
        let next_cursor = NullableRecord::read(buffer)?;
        Ok(V0Response {
            correlation_id,
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}
impl Writable for V0Response {
    fn write(&self, buffer: &mut impl BufMut) {
        self.correlation_id.write(buffer);
        TaggedFields::write_empty(buffer);
        self.throttle_time_ms.write(buffer);
        self.topics.write(buffer);
        self.next_cursor.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

pub struct DescribeTopicPartitionsResponseTopic {
    error_code: Int16,
    name: CompactNullableKafkaString,
    topic_id: KafkaUuid,
    is_internal: Boolean,
    partitions: CompactArray<DescribeTopicPartitionsResponsePartition>,
    topic_authorized_operations: Int32,
}
impl Readable for DescribeTopicPartitionsResponseTopic {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let error_code = Int16::read(buffer)?;
        let name = CompactNullableKafkaString::read(buffer)?;
        let topic_id = KafkaUuid::read(buffer)?;
        let is_internal = Boolean::read(buffer)?;
        let partitions = CompactArray::read(buffer)?;
        if partitions.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field topic was serialized as null",
            ));
        }
        let topic_authorized_operations = Int32::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
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
        self.name.write(buffer);
        self.topic_id.write(buffer);
        self.is_internal.write(buffer);
        self.partitions.write(buffer);
        self.topic_authorized_operations.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

pub struct DescribeTopicPartitionsResponsePartition {
    error_code: Int16,
    partition_index: Int32,
    leader_id: Int32,
    leader_epoch: Int32,
    replica_nodes: CompactArray<Int32>,
    isr_nodes: CompactArray<Int32>,
    eligible_leader_replicas: CompactArray<Int32>,
    last_known_elr: CompactArray<Int32>,
    offline_replicas: CompactArray<Int32>,
}
impl Readable for DescribeTopicPartitionsResponsePartition {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let error_code = Int16::read(buffer)?;
        let partition_index = Int32::read(buffer)?;
        let leader_id = Int32::read(buffer)?;
        let leader_epoch = Int32::read(buffer)?;
        let replica_nodes = CompactArray::read(buffer)?;
        if replica_nodes.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field replicaNodes was serialized as null",
            ));
        }
        let isr_nodes = CompactArray::read(buffer)?;
        if isr_nodes.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field isrNodes was serialized as null",
            ));
        }
        let eligible_leader_replicas = CompactArray::read(buffer)?;
        let last_known_elr = CompactArray::read(buffer)?;
        let offline_replicas = CompactArray::read(buffer)?;
        if offline_replicas.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field offlineReplicas was serialized as null",
            ));
        }
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
        self.replica_nodes.write(buffer);
        self.isr_nodes.write(buffer);
        self.eligible_leader_replicas.write(buffer);
        self.last_known_elr.write(buffer);
        self.offline_replicas.write(buffer);
        TaggedFields::write_empty(buffer);
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
                    .value()
                    .unwrap()
                    .iter()
                    .map(|tr| DescribeTopicPartitionsResponseTopic {
                        error_code: 3.into(),
                        name: tr.name.as_str().ok().into(),
                        topic_id: uuid::Uuid::nil().into(),
                        is_internal: false.into(),
                        partitions: vec![].into(),
                        topic_authorized_operations: 0xdf8.into(),
                    })
                    .collect::<Vec<_>>()
                    .into();
                Response::V0(V0Response {
                    correlation_id: correlation_id.into(),
                    throttle_time_ms: 0.into(),
                    topics,
                    next_cursor: None.into(),
                })
            }
        },
        _ => todo!(),
    }
}
