#![allow(clippy::too_many_arguments)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::protocol::{
    self,
    r#type::{Array, CompactArray, CompactKafkaString, KafkaString, TaggedField, TaggedFields},
    Readable, ReadableVersion, Writable,
};

use super::TopicID;

#[derive(Debug, Clone)]
pub struct Request {
    pub(super) version: i16,
    pub(super) replica_id: i32,
    pub(super) max_wait_ms: i32,
    pub(super) min_bytes: i32,
    pub(super) max_bytes: i32,
    pub(super) isolation_level: i8,
    pub(super) session_id: i32,
    pub(super) session_epoch: i32,
    pub(super) topics: Vec<FetchTopic>,
    pub(super) forgotten_topics_data: Option<Vec<ForgottenTopic>>,
    pub(super) rack_id: Option<Bytes>,

    pub(super) cluster_id: Option<Bytes>,
    pub(super) replica_state: Option<ReplicaState>,
}
impl Request {
    pub fn v4(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        topics: Vec<FetchTopic>,
    ) -> Self {
        Self {
            version: 4,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id: 0,
            session_epoch: -1,
            topics,
            forgotten_topics_data: None,
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v5(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        topics: Vec<FetchTopic>,
    ) -> Self {
        Self {
            version: 5,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id: 0,
            session_epoch: -1,
            topics,
            forgotten_topics_data: None,
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v6(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        topics: Vec<FetchTopic>,
    ) -> Self {
        Self {
            version: 6,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id: 0,
            session_epoch: -1,
            topics,
            forgotten_topics_data: None,
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v7(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
    ) -> Self {
        Self {
            version: 7,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v8(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
    ) -> Self {
        Self {
            version: 8,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v9(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
    ) -> Self {
        Self {
            version: 9,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v10(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
    ) -> Self {
        Self {
            version: 10,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id: None,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v11(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
    ) -> Self {
        Self {
            version: 11,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id: None,
            replica_state: None,
        }
    }

    pub fn v12(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
    ) -> Self {
        Self {
            version: 12,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: None,
        }
    }

    pub fn v13(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
    ) -> Self {
        Self {
            version: 13,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: None,
        }
    }

    pub fn v14(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
    ) -> Self {
        Self {
            version: 14,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: None,
        }
    }

    pub fn v15(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
        replica_state: ReplicaState,
    ) -> Self {
        Self {
            version: 15,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: Some(replica_state),
        }
    }

    pub fn v16(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
        replica_state: ReplicaState,
    ) -> Self {
        Self {
            version: 16,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: Some(replica_state),
        }
    }

    pub fn v17(
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopic>,
        rack_id: Option<Bytes>,
        cluster_id: Option<Bytes>,
        replica_state: ReplicaState,
    ) -> Self {
        Self {
            version: 17,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: Some(forgotten_topics_data),
            rack_id,

            cluster_id,
            replica_state: Some(replica_state),
        }
    }

    pub fn replica_id(&self) -> i32 {
        self.replica_id
    }

    pub fn max_wait_ms(&self) -> i32 {
        self.max_wait_ms
    }

    pub fn min_bytes(&self) -> i32 {
        self.min_bytes
    }

    pub fn max_bytes(&self) -> i32 {
        self.max_bytes
    }

    pub fn isolation_level(&self) -> i8 {
        self.isolation_level
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn session_epoch(&self) -> i32 {
        self.session_epoch
    }

    pub fn topics(&self) -> &[FetchTopic] {
        self.topics.as_ref()
    }

    pub fn forgotten_topics_data(&self) -> Option<&[ForgottenTopic]> {
        self.forgotten_topics_data.as_deref()
    }

    pub fn rack_id(&self) -> Option<&[u8]> {
        self.rack_id.as_deref()
    }

    pub fn cluster_id(&self) -> Option<&[u8]> {
        self.cluster_id.as_deref()
    }

    pub fn replica_state(&self) -> Option<&ReplicaState> {
        self.replica_state.as_ref()
    }
}
impl ReadableVersion for Request {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        if !(4..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let replica_id = if version <= 14 { i32::read(buffer) } else { -1 };
        let max_wait_ms = i32::read(buffer);
        let min_bytes = i32::read(buffer);
        let max_bytes = i32::read(buffer);
        let isolation_level = i8::read(buffer);
        let session_id = if version >= 7 { i32::read(buffer) } else { 0 };
        let session_epoch = if version >= 7 { i32::read(buffer) } else { -1 };
        let topics = if version <= 11 {
            Array::<FetchTopic>::read_version_inner(buffer, version)
        } else {
            CompactArray::<FetchTopic>::read_version_inner(buffer, version)
        }?
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field topics was serialized as null",
        ))?;
        let forgotten_topics_data = if version >= 7 {
            Some(
                if version <= 11 {
                    Array::<ForgottenTopic>::read_version_inner(buffer, version)
                } else {
                    CompactArray::<ForgottenTopic>::read_version_inner(buffer, version)
                }?
                .ok_or(protocol::Error::IllegalArgument(
                    "non-nullable field forgotten_topics_data was serialized as null",
                ))?,
            )
        } else {
            None
        };
        let rack_id = if version >= 11 {
            if version <= 11 {
                KafkaString::read_inner(buffer)
            } else {
                CompactKafkaString::read_result_inner(buffer)?
            }
        } else {
            None
        };

        let mut cluster_id = None;
        let mut replica_state = None;
        if version >= 12 {
            let tagged_fields = TaggedFields::read_result_inner(buffer)?;
            for tf in tagged_fields {
                let mut data = tf.data;
                match tf.key {
                    0 => {
                        if let Some(v) = CompactKafkaString::read_result_inner(buffer)? {
                            cluster_id.replace(v);
                        }
                    }
                    1 => {
                        if (15..=17).contains(&version) {
                            replica_state.replace(ReplicaState::read_version(&mut data, version)?);
                        }
                    }
                    _ => continue,
                }
            }
        }

        Ok(Self {
            version,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,

            cluster_id,
            replica_state,
        })
    }
}
impl Writable for Request {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        if self.version <= 14 {
            self.replica_id.write(buffer);
        }
        self.max_wait_ms.write(buffer);
        self.min_bytes.write(buffer);
        self.max_bytes.write(buffer);
        self.isolation_level.write(buffer);
        if self.version >= 7 {
            self.session_id.write(buffer);
            self.session_epoch.write(buffer);
        }
        if self.version <= 11 {
            Array::<FetchTopic>::write_inner(buffer, Some(self.topics()));
        } else {
            CompactArray::<FetchTopic>::write_inner(buffer, Some(self.topics()));
        }
        if self.version >= 7 {
            if self.version <= 11 {
                Array::<ForgottenTopic>::write_inner(buffer, self.forgotten_topics_data());
            } else {
                CompactArray::<ForgottenTopic>::write_inner(buffer, self.forgotten_topics_data());
            }
        }
        if self.version >= 11 {
            if self.version <= 11 {
                KafkaString::write_inner(buffer, self.rack_id());
            } else {
                CompactKafkaString::write_inner(buffer, self.rack_id());
            }
        }

        if self.version >= 12 {
            let mut tagged_fields = Vec::new();

            let mut data = BytesMut::with_capacity(8);
            CompactKafkaString::write_inner(&mut data, self.cluster_id());
            tagged_fields.push(TaggedField::new(0, data.freeze()));

            if self.version >= 15 {
                let mut data = BytesMut::with_capacity(12);
                if let Some(replica_state) = &self.replica_state {
                    replica_state.write(&mut data)
                };
                tagged_fields.push(TaggedField::new(1, data.freeze()));
            }

            TaggedFields::write_inner(buffer, &tagged_fields);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaState {
    replica_id: i32,
    replica_epoch: i64,
}
impl ReplicaState {
    fn new(replica_id: i32, replica_epoch: i64) -> Self {
        Self {
            replica_id,
            replica_epoch,
        }
    }

    pub fn v15(replica_id: i32, replica_epoch: i64) -> Self {
        Self::new(replica_id, replica_epoch)
    }

    pub fn v16(replica_id: i32, replica_epoch: i64) -> Self {
        Self::new(replica_id, replica_epoch)
    }

    pub fn v17(replica_id: i32, replica_epoch: i64) -> Self {
        Self::new(replica_id, replica_epoch)
    }

    pub fn replica_id(&self) -> i32 {
        self.replica_id
    }

    pub fn replica_epoch(&self) -> i64 {
        self.replica_epoch
    }
}
impl ReadableVersion for ReplicaState {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        if !(15..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let replica_id = i32::read(buffer);
        let replica_epoch = i64::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self {
            replica_id,
            replica_epoch,
        })
    }
}
impl Writable for ReplicaState {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        self.replica_id.write(buffer);
        self.replica_epoch.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct FetchTopic {
    version: i16,
    pub(super) topic: TopicID,
    pub(super) partitions: Vec<FetchPartition>,
}
impl FetchTopic {
    pub fn v4(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 4,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v5(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 5,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v6(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 6,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v7(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 7,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v8(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 8,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v9(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 9,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v10(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 10,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v11(topic: &str, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 11,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v12(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 12,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v13(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 13,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v14(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 14,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v15(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 15,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v16(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 16,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v17(topic: Uuid, partitions: Vec<FetchPartition>) -> Self {
        Self {
            version: 17,
            topic: TopicID::Id(topic),
            partitions,
        }
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

    pub fn partitions(&self) -> &[FetchPartition] {
        self.partitions.as_ref()
    }
}
impl ReadableVersion for FetchTopic {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
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
            Array::<FetchPartition>::read_version_inner(buffer, version)?
        } else {
            CompactArray::<FetchPartition>::read_version_inner(buffer, version)?
        }
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field partitions was serialized as null",
        ))?;
        if version >= 12 {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }

        Ok(Self {
            version,
            topic,
            partitions,
        })
    }
}
impl Writable for FetchTopic {
    fn write<B: BufMut>(&self, buffer: &mut B) {
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
            Array::<FetchPartition>::write_inner(buffer, Some(self.partitions()));
        } else {
            CompactArray::<FetchPartition>::write_inner(buffer, Some(self.partitions()));
        }

        if self.version >= 12 {
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchPartition {
    version: i16,
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,

    replica_directory_id: Uuid,
}
impl FetchPartition {
    pub fn v4(partition: i32, fetch_offset: i64, partition_max_bytes: i32) -> Self {
        Self {
            version: 4,
            partition,
            current_leader_epoch: -1,
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v5(
        partition: i32,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 5,
            partition,
            current_leader_epoch: -1,
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v6(
        partition: i32,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 6,
            partition,
            current_leader_epoch: -1,
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v7(
        partition: i32,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 7,
            partition,
            current_leader_epoch: -1,
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v8(
        partition: i32,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 7,
            partition,
            current_leader_epoch: -1,
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v9(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 9,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v10(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 10,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v11(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 11,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch: -1,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v12(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 12,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v13(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 13,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v14(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 14,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v15(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 15,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v16(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
    ) -> Self {
        Self {
            version: 16,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id: Uuid::nil(),
        }
    }

    pub fn v17(
        partition: i32,
        current_leader_epoch: Option<i32>,
        fetch_offset: i64,
        last_fetched_epoch: i32,
        log_start_offset: Option<i64>,
        partition_max_bytes: i32,
        replica_directory_id: Uuid,
    ) -> Self {
        Self {
            version: 17,
            partition,
            current_leader_epoch: current_leader_epoch.unwrap_or(-1),
            fetch_offset,
            last_fetched_epoch,
            log_start_offset: log_start_offset.unwrap_or(-1),
            partition_max_bytes,

            replica_directory_id,
        }
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn current_leader_epoch(&self) -> i32 {
        self.current_leader_epoch
    }

    pub fn fetch_offset(&self) -> i64 {
        self.fetch_offset
    }

    pub fn last_fetched_epoch(&self) -> i32 {
        self.last_fetched_epoch
    }

    pub fn log_start_offset(&self) -> i64 {
        self.log_start_offset
    }

    pub fn partition_max_bytes(&self) -> i32 {
        self.partition_max_bytes
    }

    pub fn replica_directory_id(&self) -> Uuid {
        self.replica_directory_id
    }
}
impl ReadableVersion for FetchPartition {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        if !(4..=17).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let partition = i32::read(buffer);
        let current_leader_epoch = if version >= 9 { i32::read(buffer) } else { -1 };
        let fetch_offset = i64::read(buffer);
        let last_fetched_epoch = if version >= 12 { i32::read(buffer) } else { -1 };
        let log_start_offset = if version >= 5 { i64::read(buffer) } else { -1 };
        let partition_max_bytes = i32::read(buffer);

        let mut replica_directory_id = Uuid::nil();
        let tagged_fields = TaggedFields::read_result_inner(buffer)?;
        for tf in tagged_fields {
            let mut data = tf.data;
            match tf.key {
                0 => {
                    if version >= 17 {
                        replica_directory_id = Uuid::read(&mut data);
                    }
                }
                _ => continue,
            }
        }

        Ok(Self {
            version,
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            replica_directory_id,
        })
    }
}
impl Writable for FetchPartition {
    fn write<B: BufMut>(&self, buffer: &mut B) {
        self.partition.write(buffer);
        if self.version >= 9 {
            self.current_leader_epoch.write(buffer);
        }
        self.fetch_offset.write(buffer);
        if self.version >= 12 {
            self.last_fetched_epoch.write(buffer);
        }
        if self.version >= 5 {
            self.log_start_offset.write(buffer);
        }
        self.partition_max_bytes.write(buffer);

        if self.version >= 12 {
            if self.version >= 17 {
                let mut data = BytesMut::with_capacity(16);
                self.replica_directory_id.write(&mut data);
                TaggedFields::write_inner(buffer, &[TaggedField::new(0, data.freeze())]);
            } else {
                TaggedFields::write_empty(buffer);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForgottenTopic {
    version: i16,
    topic: TopicID,
    partitions: Vec<i32>,
}
impl ForgottenTopic {
    pub fn v7(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 7,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v8(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 8,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v9(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 9,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v10(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 10,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v11(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 11,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v12(topic: &str, partitions: Vec<i32>) -> Self {
        Self {
            version: 12,
            topic: TopicID::Name(Bytes::copy_from_slice(topic.as_bytes())),
            partitions,
        }
    }

    pub fn v13(topic: Uuid, partitions: Vec<i32>) -> Self {
        Self {
            version: 13,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v14(topic: Uuid, partitions: Vec<i32>) -> Self {
        Self {
            version: 14,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v15(topic: Uuid, partitions: Vec<i32>) -> Self {
        Self {
            version: 15,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v16(topic: Uuid, partitions: Vec<i32>) -> Self {
        Self {
            version: 16,
            topic: TopicID::Id(topic),
            partitions,
        }
    }

    pub fn v17(topic: Uuid, partitions: Vec<i32>) -> Self {
        Self {
            version: 17,
            topic: TopicID::Id(topic),
            partitions,
        }
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

    pub fn partitions(&self) -> &[i32] {
        self.partitions.as_ref()
    }
}
impl ReadableVersion for ForgottenTopic {
    fn read_version<B: Buf>(buffer: &mut B, version: i16) -> Result<Self, protocol::Error> {
        if !(7..=17).contains(&version) {
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
            Array::<i32>::read_inner(buffer)
        } else {
            CompactArray::<i32>::read_inner(buffer)?
        }
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field partitions was serialized as null",
        ))?;
        if version >= 12 {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }

        Ok(Self {
            version,
            topic,
            partitions,
        })
    }
}
impl Writable for ForgottenTopic {
    fn write<B: BufMut>(&self, buffer: &mut B) {
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
            Array::<i32>::write_inner(buffer, Some(self.partitions()));
        } else {
            CompactArray::<i32>::write_inner(buffer, Some(self.partitions()));
        }
        if self.version >= 12 {
            TaggedFields::write_empty(buffer);
        }
    }
}
