use std::{collections::HashMap, sync::LazyLock};

use model::Topic;
use protocol::{
    message::{
        api_versions::{ApiKey, FinalizedFeature, SupportedFeature},
        fetch::read_record_batches,
    },
    Readable, ReadableVersion,
};

pub mod metadata;
pub mod model;
pub mod protocol;

pub(crate) static SUPPORTED_APIS: LazyLock<HashMap<i16, ApiKey>> = LazyLock::new(|| {
    let mut res = HashMap::new();
    res.insert(1, ApiKey::v4(1, 4, 17));
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
        for rb in read_record_batches(
            "/tmp/kraft-combined-logs",
            "__cluster_metadata-0/00000000000000000000.log",
        )? {
            for rec in rb.records() {
                match rec {
                    metadata::Record::Value(value_record) => {
                        let mut metadata_buffer = value_record.value().clone();
                        let value = metadata::MetadataValue::read(&mut metadata_buffer);
                        let r#type = value.r#type();
                        let version = value.version() as i16;
                        let mut data = value.data().clone();
                        match r#type as i16 {
                            metadata::records::topic_record::API_KEY => {
                                let topic_record = metadata::records::TopicRecord::read_version(
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
                            _ => continue,
                        }
                    }
                    metadata::Record::Control(_) => continue,
                }
            }
        }

        Ok(topics)
    });
