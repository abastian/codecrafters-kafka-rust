use std::{collections::HashMap, fs::File, io::Read, sync::LazyLock};

use model::Topic;
use protocol::{
    message::api_versions::{ApiKey, FinalizedFeature, SupportedFeature},
    ReadableResult, ReadableVersion,
};

pub mod metadata;
pub mod model;
pub mod protocol;

pub(crate) static SUPPORTED_APIS: LazyLock<HashMap<i16, ApiKey>> = LazyLock::new(|| {
    let mut res = HashMap::new();
    res.insert(1, ApiKey::v4(1, 0, 16));
    res.insert(18, ApiKey::v4(18, 0, 4));
    res.insert(75, ApiKey::v4(75, 0, 0));

    res
});

pub(crate) static SUPPORTED_FEATURES: LazyLock<HashMap<String, SupportedFeature>> =
    LazyLock::new(HashMap::new);

pub(crate) static FINALIZED_FEATURES: LazyLock<HashMap<String, FinalizedFeature>> =
    LazyLock::new(HashMap::new);

pub(crate) static METADATA_CACHE: LazyLock<Result<HashMap<uuid::Uuid, Topic>, protocol::Error>> =
    LazyLock::new(|| {
        let mut topics = HashMap::new();
        let mut buffer = vec![0u8; 8192];
        let mut file =
            File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
                .map_err(|err| protocol::Error::IOError(err.to_string()))?;
        let mut left_sz = 0usize;
        loop {
            let read_sz = file
                .read(&mut buffer[left_sz..])
                .map_err(|err| protocol::Error::IOError(err.to_string()))?;
            if read_sz == 0 {
                break;
            }
            buffer.truncate(left_sz + read_sz);

            let mut read_buffer = &buffer[0..];
            loop {
                match metadata::RecordBatch::read_result(&mut read_buffer) {
                    Ok(rb) => {
                        if rb.is_control_batch() {
                            continue;
                        }
                        for rec in rb.records() {
                            if let metadata::Record::Value(rec) = rec {
                                let value = rec.value();
                                let r#type = value.r#type();
                                let version = value.version() as i16;
                                let mut data = value.data().clone();
                                match r#type as i16 {
                                    metadata::records::topic_record::API_KEY => {
                                        let topic_record =
                                            metadata::records::TopicRecord::read_version(
                                                &mut data, version,
                                            )?;
                                        let topic_id = topic_record.topic_id();
                                        let topic_name =
                                            std::str::from_utf8(topic_record.name())?.to_string();
                                        let topic = model::Topic::new(topic_id, topic_name);
                                        topics.insert(topic_id, topic);
                                    }
                                    metadata::records::partition_record::API_KEY => {
                                        let partition_record =
                                            metadata::records::PartitionRecord::read_version(
                                                &mut data, version,
                                            )?;
                                        let topic_id = partition_record.topic_id();
                                        let partition = model::Partition::new(
                                            partition_record.partition_id(),
                                            partition_record.leader(),
                                            partition_record.leader_epoch(),
                                            partition_record.replicas().to_vec(),
                                            partition_record.isr().to_vec(),
                                            partition_record
                                                .eligible_leader_replicas()
                                                .map(|v| v.to_vec()),
                                            partition_record.last_known_elr().map(|v| v.to_vec()),
                                        );
                                        if let Some(topic) = topics.get_mut(&topic_id) {
                                            topic.add_partition(partition);
                                        }
                                    }
                                    _ => {
                                        continue;
                                    }
                                }
                            } else {
                                unreachable!()
                            }
                        }
                    }
                    Err(protocol::Error::BufferUnderflow) => {
                        break;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }

            left_sz = read_buffer.len();
            buffer.copy_within(left_sz.., 0);
        }

        Ok(topics)
    });
