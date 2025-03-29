use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::protocol::{
    self,
    r#type::{Array, CompactArray, CompactKafkaString, KafkaString, TaggedField, TaggedFields},
    Readable, ReadableVersion, Writable,
};

use super::TopicID;

#[derive(Debug, Clone)]
pub struct Response {
    pub(super) version: i16,
    pub(super) throttle_time_ms: i32,
    pub(super) error_code: i16,
    pub(super) session_id: i32,
    pub(super) responses: Vec<FetchableTopicResponse>,
    pub(super) node_endpoints: Option<Vec<NodeEndpoint>>,
}
impl Response {
    fn new(
        version: i16,
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
        node_endpoints: Option<Vec<NodeEndpoint>>,
    ) -> Self {
        Self {
            version,
            throttle_time_ms: throttle_time_ms.unwrap_or(-1),
            error_code: error_code.unwrap_or(0),
            session_id,
            responses,
            node_endpoints,
        }
    }

    pub fn v4(throttle_time_ms: Option<i32>, responses: Vec<FetchableTopicResponse>) -> Self {
        Self::new(4, throttle_time_ms, None, 0, responses, None)
    }

    pub fn v5(throttle_time_ms: Option<i32>, responses: Vec<FetchableTopicResponse>) -> Self {
        Self::new(5, throttle_time_ms, None, 0, responses, None)
    }

    pub fn v6(throttle_time_ms: Option<i32>, responses: Vec<FetchableTopicResponse>) -> Self {
        Self::new(6, throttle_time_ms, None, 0, responses, None)
    }

    pub fn v7(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(7, throttle_time_ms, error_code, session_id, responses, None)
    }

    pub fn v8(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(8, throttle_time_ms, error_code, session_id, responses, None)
    }

    pub fn v9(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(9, throttle_time_ms, error_code, session_id, responses, None)
    }

    pub fn v10(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            10,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v11(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            11,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v12(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            12,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v13(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            13,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v14(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            14,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v15(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
    ) -> Self {
        Self::new(
            15,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            None,
        )
    }

    pub fn v16(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
        node_endpoints: Option<Vec<NodeEndpoint>>,
    ) -> Self {
        Self::new(
            16,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        )
    }

    pub fn v17(
        throttle_time_ms: Option<i32>,
        error_code: Option<i16>,
        session_id: i32,
        responses: Vec<FetchableTopicResponse>,
        node_endpoints: Option<Vec<NodeEndpoint>>,
    ) -> Self {
        Self::new(
            17,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        )
    }

    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
    }

    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn responses(&self) -> &[FetchableTopicResponse] {
        self.responses.as_ref()
    }

    pub fn node_endpoints(&self) -> Option<&[NodeEndpoint]> {
        self.node_endpoints.as_deref()
    }
}
impl ReadableVersion for Response {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(7..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let throttle_time_ms = i32::read(buffer);
        let error_code = if version >= 7 { i16::read(buffer) } else { 0 };
        let session_id = if version >= 7 { i32::read(buffer) } else { 0 };
        let responses = if version <= 11 {
            Array::<FetchableTopicResponse>::read_version_inner(buffer, version)
        } else {
            CompactArray::<FetchableTopicResponse>::read_version_inner(buffer, version)
        }?
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field responses was serialized as null",
        ))?;

        let mut node_endpoints = None;
        if version >= 12 {
            let tagged_fields = TaggedFields::read_result_inner(buffer)?;

            for tf in tagged_fields {
                let mut data = tf.data;
                match tf.key {
                    0 => {
                        node_endpoints.replace(
                            CompactArray::<NodeEndpoint>::read_version_inner(&mut data, version)?
                                .ok_or(protocol::Error::IllegalArgument(
                                "non-nullable field node_endpoints was serialized as null",
                            ))?,
                        );
                    }
                    _ => continue,
                }
            }
        }

        Ok(Self {
            version,
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        })
    }
}
impl Writable for Response {
    fn write(&self, buffer: &mut impl BufMut) {
        self.throttle_time_ms.write(buffer);
        if self.version >= 7 {
            self.error_code.write(buffer);
            self.session_id.write(buffer);
        }
        if self.version <= 11 {
            Array::<FetchableTopicResponse>::write_inner(buffer, Some(self.responses()));
        } else {
            CompactArray::<FetchableTopicResponse>::write_inner(buffer, Some(self.responses()));
        }

        if self.version >= 12 {
            if let Some(node_endpoints) = self.node_endpoints() {
                let mut data = BytesMut::with_capacity(16);
                CompactArray::<NodeEndpoint>::write_inner(&mut data, Some(node_endpoints));
                TaggedFields::write_inner(buffer, &[TaggedField::new(0, data.freeze())]);
            } else {
                TaggedFields::write_empty(buffer);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchableTopicResponse {
    version: i16,
    topic: TopicID,
    partitions: Vec<PartitionData>,
}
impl FetchableTopicResponse {
    fn new(version: i16, topic: TopicID, partitions: Vec<PartitionData>) -> Self {
        Self {
            version,
            topic,
            partitions,
        }
    }

    pub fn v4(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            4,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v5(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            5,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v6(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            6,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v7(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            7,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v8(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            8,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v9(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            9,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v10(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            10,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v11(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            11,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v12(topic: &str, partitions: Vec<PartitionData>) -> Self {
        Self::new(
            12,
            TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        )
    }

    pub fn v13(topic: Uuid, partitions: Vec<PartitionData>) -> Self {
        Self::new(13, TopicID::Id(topic), partitions)
    }

    pub fn v14(topic: Uuid, partitions: Vec<PartitionData>) -> Self {
        Self::new(14, TopicID::Id(topic), partitions)
    }

    pub fn v15(topic: Uuid, partitions: Vec<PartitionData>) -> Self {
        Self::new(15, TopicID::Id(topic), partitions)
    }

    pub fn v16(topic: Uuid, partitions: Vec<PartitionData>) -> Self {
        Self::new(16, TopicID::Id(topic), partitions)
    }

    pub fn v17(topic: Uuid, partitions: Vec<PartitionData>) -> Self {
        Self::new(17, TopicID::Id(topic), partitions)
    }

    pub fn topic(&self) -> Option<&[u8]> {
        if let TopicID::Name(name) = &self.topic {
            Some(name.as_ref())
        } else {
            None
        }
    }

    pub fn topic_id(&self) -> Uuid {
        if let TopicID::Id(id) = &self.topic {
            *id
        } else {
            Uuid::nil()
        }
    }

    pub fn partitions(&self) -> &[PartitionData] {
        self.partitions.as_ref()
    }
}
impl ReadableVersion for FetchableTopicResponse {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(4..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let topic = if version <= 12 {
            TopicID::Name(
                if version <= 11 {
                    KafkaString::read_inner(buffer)
                } else {
                    CompactKafkaString::read_result_inner(buffer)?
                }
                .ok_or(protocol::Error::IllegalArgument(
                    "non-nullable field topic was serialized as null",
                ))?,
            )
        } else {
            TopicID::Id(Uuid::read(buffer))
        };
        let partitions = if version <= 11 {
            Array::<PartitionData>::read_version_inner(buffer, version)
        } else {
            CompactArray::<PartitionData>::read_version_inner(buffer, version)
        }?
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field partitions was serialized as null",
        ))?;

        Ok(Self {
            version,
            topic,
            partitions,
        })
    }
}
impl Writable for FetchableTopicResponse {
    fn write(&self, buffer: &mut impl BufMut) {
        match &self.topic {
            TopicID::Name(bytes) => {
                if self.version <= 11 {
                    KafkaString::write_inner(buffer, Some(bytes.as_ref()));
                } else {
                    CompactKafkaString::write_inner(buffer, Some(bytes.as_ref()));
                }
            }
            TopicID::Id(uuid) => uuid.write(buffer),
        }
        if self.version <= 11 {
            Array::<PartitionData>::write_inner(buffer, Some(self.partitions()));
        } else {
            CompactArray::<PartitionData>::write_inner(buffer, Some(self.partitions()));
        }

        if self.version >= 12 {
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionData {
    version: i16,
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Option<Vec<AbortedTransaction>>,
    preferred_read_replica: i32,
    records: Option<Bytes>,
    diverging_epoch: Option<EpochEndOffset>,
    current_leader: Option<LeaderIdAndEpoch>,
    snapshot_id: Option<SnapshotId>,
}
impl PartitionData {
    fn new(
        version: i16,
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self {
            version,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset: last_stable_offset.unwrap_or(-1),
            log_start_offset: log_start_offset.unwrap_or(-1),
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        }
    }

    pub fn v4(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            4,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            None,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v5(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            5,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v6(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            6,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v7(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            7,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v8(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            8,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v9(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            9,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v10(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            10,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            -1,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v11(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
    ) -> Self {
        Self::new(
            11,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            None,
            None,
            None,
        )
    }

    pub fn v12(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            12,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn v13(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            13,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn v14(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            14,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn v15(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            15,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn v16(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            16,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn v17(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: Option<i64>,
        log_start_offset: Option<i64>,
        aborted_transactions: Option<Vec<AbortedTransaction>>,
        preferred_read_replica: i32,
        records: Option<Bytes>,
        diverging_epoch: Option<EpochEndOffset>,
        current_leader: Option<LeaderIdAndEpoch>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self::new(
            17,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        )
    }

    pub fn aborted_transactions(&self) -> Option<&[AbortedTransaction]> {
        self.aborted_transactions.as_deref()
    }
}
impl ReadableVersion for PartitionData {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if version < 4 || version > 17 {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let partition_index = i32::read(buffer);
        let error_code = i16::read(buffer);
        let high_watermark = i64::read(buffer);
        let last_stable_offset = i64::read(buffer);
        let log_start_offset = if version >= 5 { i64::read(buffer) } else { -1 };
        let aborted_transactions = if version <= 11 {
            Array::<AbortedTransaction>::read_version_inner(buffer, version)
        } else {
            CompactArray::<AbortedTransaction>::read_version_inner(buffer, version)
        }?;
        let preferred_read_replica = if version >= 11 { i32::read(buffer) } else { -1 };
        let records = if version <= 11 {
            KafkaString::read_inner(buffer)
        } else {
            CompactKafkaString::read_result_inner(buffer)?
        };

        let mut diverging_epoch = None;
        let mut current_leader = None;
        let mut snapshot_id = None;
        if version >= 12 {
            let tagged_fields = TaggedFields::read_result_inner(buffer)?;
            for tf in tagged_fields {
                let mut data = tf.data;
                match tf.key {
                    0 => {
                        diverging_epoch.replace(EpochEndOffset::read_version(&mut data, version)?);
                    }
                    1 => {
                        current_leader.replace(LeaderIdAndEpoch::read_version(&mut data, version)?);
                    }
                    2 => {
                        snapshot_id.replace(SnapshotId::read_version(&mut data, version)?);
                    }
                    _ => continue,
                }
            }
        }

        Ok(Self {
            version,
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
            diverging_epoch,
            current_leader,
            snapshot_id,
        })
    }
}
impl Writable for PartitionData {
    fn write(&self, buffer: &mut impl BufMut) {
        self.partition_index.write(buffer);
        self.error_code.write(buffer);
        self.high_watermark.write(buffer);
        self.last_stable_offset.write(buffer);
        if self.version >= 5 {
            self.log_start_offset.write(buffer);
        }
        if self.version <= 11 {
            Array::write_inner(buffer, self.aborted_transactions());
        } else {
            CompactArray::write_inner(buffer, self.aborted_transactions());
        }
        if self.version >= 11 {
            self.preferred_read_replica.write(buffer);
        }
        if self.version <= 11 {
            KafkaString::write_inner(buffer, self.records.as_deref());
        } else {
            CompactKafkaString::write_inner(buffer, self.records.as_deref());
        }

        if self.version >= 12 {
            let mut tagged_fields = Vec::new();
            if let Some(diverging_epoch) = &self.diverging_epoch {
                let mut data = BytesMut::with_capacity(12);
                diverging_epoch.write(&mut data);
                tagged_fields.push(TaggedField::new(0, data.freeze()));
            }

            if let Some(current_leader) = &self.current_leader {
                let mut data = BytesMut::with_capacity(8);
                current_leader.write(&mut data);
                tagged_fields.push(TaggedField::new(1, data.freeze()));
            }

            if let Some(snapshot_id) = &self.snapshot_id {
                let mut data = BytesMut::with_capacity(12);
                snapshot_id.write(&mut data);
                tagged_fields.push(TaggedField::new(2, data.freeze()));
            }

            TaggedFields::write_inner(buffer, &tagged_fields);
        } else {
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    version: i16,
    producer_id: i64,
    first_offset: i64,
}
impl AbortedTransaction {
    pub fn v4(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 4,
            producer_id,
            first_offset,
        }
    }

    pub fn v5(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 5,
            producer_id,
            first_offset,
        }
    }

    pub fn v6(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 6,
            producer_id,
            first_offset,
        }
    }

    pub fn v7(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 7,
            producer_id,
            first_offset,
        }
    }

    pub fn v8(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 8,
            producer_id,
            first_offset,
        }
    }

    pub fn v9(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 9,
            producer_id,
            first_offset,
        }
    }

    pub fn v10(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 10,
            producer_id,
            first_offset,
        }
    }

    pub fn v11(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 11,
            producer_id,
            first_offset,
        }
    }

    pub fn v12(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 12,
            producer_id,
            first_offset,
        }
    }

    pub fn v13(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 13,
            producer_id,
            first_offset,
        }
    }

    pub fn v14(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 14,
            producer_id,
            first_offset,
        }
    }

    pub fn v15(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 15,
            producer_id,
            first_offset,
        }
    }

    pub fn v16(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 16,
            producer_id,
            first_offset,
        }
    }

    pub fn v17(producer_id: i64, first_offset: i64) -> Self {
        Self {
            version: 17,
            producer_id,
            first_offset,
        }
    }

    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    pub fn first_offset(&self) -> i64 {
        self.first_offset
    }
}
impl ReadableVersion for AbortedTransaction {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(4..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let producer_id = i64::read(buffer);
        let first_offset = i64::read(buffer);
        if version >= 12 {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }

        Ok(Self {
            version,
            producer_id,
            first_offset,
        })
    }
}
impl Writable for AbortedTransaction {
    fn write(&self, buffer: &mut impl BufMut) {
        self.producer_id.write(buffer);
        self.first_offset.write(buffer);

        if self.version >= 12 {
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpochEndOffset {
    epoch: i32,
    end_offset: i64,
}
impl EpochEndOffset {
    pub fn new(epoch: i32, end_offset: i64) -> Self {
        Self { epoch, end_offset }
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }
}
impl ReadableVersion for EpochEndOffset {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(12..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let epoch = i32::read(buffer);
        let end_offset = i64::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self { epoch, end_offset })
    }
}
impl Writable for EpochEndOffset {
    fn write(&self, buffer: &mut impl BufMut) {
        self.epoch.write(buffer);
        self.end_offset.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct LeaderIdAndEpoch {
    leader_id: i32,
    leader_epoch: i32,
}
impl LeaderIdAndEpoch {
    pub fn new(leader_id: i32, leader_epoch: i32) -> Self {
        Self {
            leader_id,
            leader_epoch,
        }
    }

    pub fn leader_id(&self) -> i32 {
        self.leader_id
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }
}
impl ReadableVersion for LeaderIdAndEpoch {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(12..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let leader_id = i32::read(buffer);
        let leader_epoch = i32::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self {
            leader_id,
            leader_epoch,
        })
    }
}
impl Writable for LeaderIdAndEpoch {
    fn write(&self, buffer: &mut impl BufMut) {
        self.leader_id.write(buffer);
        self.leader_epoch.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotId {
    end_offset: i64,
    epoch: i32,
}
impl SnapshotId {
    pub fn new(end_offset: i64, epoch: i32) -> Self {
        Self { end_offset, epoch }
    }

    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }
}
impl ReadableVersion for SnapshotId {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(12..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let end_offset = i64::read(buffer);
        let epoch = i32::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self { end_offset, epoch })
    }
}
impl Writable for SnapshotId {
    fn write(&self, buffer: &mut impl BufMut) {
        self.end_offset.write(buffer);
        self.epoch.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    node_id: i32,
    host: Bytes,
    port: i32,
    rack: Option<Bytes>,
}
impl NodeEndpoint {
    pub fn new(node_id: i32, host: Bytes, port: i32, rack: Option<Bytes>) -> Self {
        Self {
            node_id,
            host,
            port,
            rack,
        }
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn host(&self) -> &[u8] {
        self.host.as_ref()
    }

    pub fn port(&self) -> i32 {
        self.port
    }

    pub fn rack(&self) -> Option<&[u8]> {
        self.rack.as_deref()
    }
}
impl ReadableVersion for NodeEndpoint {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(16..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let node_id = i32::read(buffer);
        let host = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field host was serialized as null"),
        )?;
        let port = i32::read(buffer);
        let rack = CompactKafkaString::read_result_inner(buffer)?;
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}
impl Writable for NodeEndpoint {
    fn write(&self, buffer: &mut impl BufMut) {
        self.node_id.write(buffer);
        CompactKafkaString::write_inner(buffer, Some(self.host()));
        self.port.write(buffer);
        CompactKafkaString::write_inner(buffer, self.rack());
        TaggedFields::write_empty(buffer);
    }
}
