use bytes::{Buf, BufMut};

use crate::protocol::{
    self,
    r#type::{
        Boolean, CompactArray, CompactKafkaString, CompactNullableKafkaString, Int16, Int32,
        KafkaUuid, NullableRecord, TaggedFields,
    },
    Readable, Writable,
};

#[derive(Debug, Clone)]
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
            topics: Some(topics).into(),
            response_partition_limit: response_partition_limit.into(),
            cursor: cursor.into(),
        }
    }

    pub fn topics(&self) -> &[TopicRequest] {
        self.topics.value().unwrap()
    }

    pub fn response_partition_limit(&self) -> Int32 {
        self.response_partition_limit
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.value()
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

#[derive(Debug, Clone)]
pub struct TopicRequest {
    name: CompactKafkaString,
}
impl TopicRequest {
    pub fn new(name: &str) -> Self {
        TopicRequest { name: name.into() }
    }

    pub fn name(&self) -> &CompactKafkaString {
        &self.name
    }
}
impl Readable for TopicRequest {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let name = CompactKafkaString::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;
        Ok(TopicRequest { name })
    }
}

#[derive(Debug, Clone)]
pub struct Cursor {
    topic_name: CompactKafkaString,
    partition_index: Int32,
}
impl Cursor {
    pub fn new(topic_name: &str, partition_index: i32) -> Self {
        Cursor {
            topic_name: topic_name.into(),
            partition_index: partition_index.into(),
        }
    }

    pub fn topic_name(&self) -> &CompactKafkaString {
        &self.topic_name
    }

    pub fn partition_index(&self) -> Int32 {
        self.partition_index
    }
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

#[derive(Debug, Clone)]
pub struct V0Response {
    correlation_id: Int32,
    throttle_time_ms: Int32,
    topics: CompactArray<DescribeTopicPartitionsResponseTopic>,
    next_cursor: NullableRecord<Cursor>,
}
impl V0Response {
    pub fn new(
        correlation_id: i32,
        throttle_time_ms: i32,
        topics: Vec<DescribeTopicPartitionsResponseTopic>,
        next_cursor: Option<Cursor>,
    ) -> Self {
        V0Response {
            correlation_id: correlation_id.into(),
            throttle_time_ms: throttle_time_ms.into(),
            topics: Some(topics).into(),
            next_cursor: next_cursor.into(),
        }
    }

    pub fn correlation_id(&self) -> Int32 {
        self.correlation_id
    }

    pub fn throttle_time_ms(&self) -> Int32 {
        self.throttle_time_ms
    }

    pub fn topics(&self) -> &[DescribeTopicPartitionsResponseTopic] {
        self.topics.value().unwrap()
    }

    pub fn next_cursor(&self) -> Option<&Cursor> {
        self.next_cursor.value()
    }
}
impl Readable for V0Response {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let correlation_id = Int32::read(buffer)?;
        let throttle_time_ms = Int32::read(buffer)?;
        let topics = CompactArray::read(buffer)?;
        if topics.value().is_none() {
            return Err(protocol::Error::IllegalArgument(
                "non-nullable field topics was serialized as null",
            ));
        }
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

#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsResponseTopic {
    error_code: Int16,
    name: CompactNullableKafkaString,
    topic_id: KafkaUuid,
    is_internal: Boolean,
    partitions: CompactArray<DescribeTopicPartitionsResponsePartition>,
    topic_authorized_operations: Int32,
}
impl DescribeTopicPartitionsResponseTopic {
    pub fn new(
        error_code: i16,
        name: Option<&str>,
        topic_id: uuid::Uuid,
        is_internal: bool,
        partitions: Vec<DescribeTopicPartitionsResponsePartition>,
        topic_authorized_operations: i32,
    ) -> Self {
        Self {
            error_code: error_code.into(),
            name: name.into(),
            topic_id: topic_id.into(),
            is_internal: is_internal.into(),
            partitions: Some(partitions).into(),
            topic_authorized_operations: topic_authorized_operations.into(),
        }
    }

    pub fn error_code(&self) -> Int16 {
        self.error_code
    }

    pub fn name(&self) -> &CompactNullableKafkaString {
        &self.name
    }

    pub fn topic_id(&self) -> KafkaUuid {
        self.topic_id
    }

    pub fn is_internal(&self) -> Boolean {
        self.is_internal
    }

    pub fn partitions(&self) -> &CompactArray<DescribeTopicPartitionsResponsePartition> {
        &self.partitions
    }

    pub fn topic_authorized_operations(&self) -> Int32 {
        self.topic_authorized_operations
    }
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

#[derive(Debug, Clone)]
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
impl DescribeTopicPartitionsResponsePartition {
    pub fn new(
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
            error_code: error_code.into(),
            partition_index: partition_index.into(),
            leader_id: leader_id.into(),
            leader_epoch: leader_epoch.into(),
            replica_nodes: Some(
                replica_nodes
                    .iter()
                    .map(|&id| id.into())
                    .collect::<Vec<Int32>>(),
            )
            .into(),
            isr_nodes: Some(
                isr_nodes
                    .iter()
                    .map(|&id| id.into())
                    .collect::<Vec<Int32>>(),
            )
            .into(),
            eligible_leader_replicas: eligible_leader_replicas
                .map(|el| el.iter().map(|&id| id.into()).collect::<Vec<Int32>>())
                .into(),
            last_known_elr: last_known_elr
                .map(|el| el.iter().map(|&id| id.into()).collect::<Vec<Int32>>())
                .into(),
            offline_replicas: Some(
                offline_replicas
                    .iter()
                    .map(|&id| id.into())
                    .collect::<Vec<Int32>>(),
            )
            .into(),
        }
    }

    pub fn error_code(&self) -> Int16 {
        self.error_code
    }

    pub fn partition_index(&self) -> Int32 {
        self.partition_index
    }

    pub fn leader_id(&self) -> Int32 {
        self.leader_id
    }

    pub fn leader_epoch(&self) -> Int32 {
        self.leader_epoch
    }

    pub fn replica_nodes(&self) -> &CompactArray<Int32> {
        &self.replica_nodes
    }

    pub fn isr_nodes(&self) -> &CompactArray<Int32> {
        &self.isr_nodes
    }

    pub fn eligible_leader_replicas(&self) -> &CompactArray<Int32> {
        &self.eligible_leader_replicas
    }

    pub fn last_known_elr(&self) -> &CompactArray<Int32> {
        &self.last_known_elr
    }

    pub fn offline_replicas(&self) -> &CompactArray<Int32> {
        &self.offline_replicas
    }
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
                let topics = Some(
                    req.topics()
                        .iter()
                        .map(|tr| DescribeTopicPartitionsResponseTopic {
                            error_code: 3.into(),
                            name: Some(tr.name.value()).into(),
                            topic_id: uuid::Uuid::nil().into(),
                            is_internal: false.into(),
                            partitions: Some(vec![]).into(),
                            topic_authorized_operations: 0xdf8.into(),
                        })
                        .collect::<Vec<_>>(),
                )
                .into();
                Response::V0(V0Response {
                    correlation_id: correlation_id.into(),
                    throttle_time_ms: 0.into(),
                    topics,
                    next_cursor: None.into(),
                })
            }
        },
        _ => unreachable!(),
    }
}
