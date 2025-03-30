use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
};

use bytes::Bytes;
use response::{FetchableTopicResponse, PartitionData};
use uuid::Uuid;

use crate::{
    metadata, model,
    protocol::{self, ReadableResult},
};

use super::topic_by_name;

pub(crate) mod request;
pub(crate) mod response;

pub(crate) const API_KEY: i16 = 1;

#[derive(Debug, Clone)]
enum TopicID {
    Name(Bytes),
    Id(Uuid),
}

pub fn process_request(
    request: request::Request,
    metadata: &HashMap<Uuid, model::Topic>,
) -> Result<response::Response, protocol::Error> {
    let version = request.version;
    let mut responses = Vec::with_capacity(request.topics.len());

    for topic_req in request.topics() {
        let topic_opt = match &topic_req.topic {
            TopicID::Name(bytes) => {
                let name = std::str::from_utf8(bytes.as_ref())?;
                topic_by_name(metadata, name)
            }
            TopicID::Id(uuid) => metadata.get(uuid),
        };

        let fetch_topic_response = match topic_opt {
            Some(topic_metadata) => {
                let partitions = {
                    let mut values = Vec::new();
                    for partition in topic_metadata.partitions() {
                        let partition_index = partition.id();
                        let rel_log_path = format!(
                            "{}-{}/00000000000000000000.log",
                            topic_metadata.name(),
                            partition_index
                        );

                        let records = {
                            let record_batches =
                                read_record_file("/tmp/kraft-combined-logs", &rel_log_path)?;
                            Some(Bytes::copy_from_slice(&record_batches))
                        };

                        values.push(PartitionData::new(
                            version,
                            partition_index,
                            0,
                            0,
                            None,
                            None,
                            None,
                            -1,
                            records,
                            None,
                            None,
                            None,
                        ))
                    }

                    values
                };
                FetchableTopicResponse::new(version, topic_req.topic.clone(), partitions)
            }
            None => FetchableTopicResponse::new(
                version,
                topic_req.topic.clone(),
                vec![PartitionData::new(
                    version, 0, 100, 0, None, None, None, -1, None, None, None, None,
                )],
            ),
        };

        responses.push(fetch_topic_response);
    }

    Ok(response::Response {
        version,
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses,
        node_endpoints: None,
    })
}

pub(crate) fn read_record_file(
    base_path: &str,
    rel_log_path: &str,
) -> Result<Vec<u8>, protocol::Error> {
    fs::read(format!("{}/{}", base_path, rel_log_path))
        .map_err(|err| protocol::Error::IOError(err.to_string()))
}

pub(crate) fn read_record_batches(
    base_path: &str,
    rel_log_path: &str,
) -> Result<Vec<metadata::RecordBatch>, protocol::Error> {
    let mut result = Vec::new();
    let mut buffer = vec![0u8; 8192];
    let mut file = File::open(format!("{}/{}", base_path, rel_log_path))
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
            //         if rb.is_control_batch() {
            //             continue;
            //         }
            //         result.append(
            //             &mut rb
            //                 .records()
            //                 .iter()
            //                 .filter_map(|r| match r {
            //                     metadata::Record::Value(value_record) => {
            //                         Some(value_record.value().clone())
            //                     }
            //                     metadata::Record::Control(_) => None,
            //                 })
            //                 .collect::<Vec<_>>(),
            //         );
            match metadata::RecordBatch::read_result(&mut read_buffer) {
                Ok(rb) => result.push(rb),
                Err(protocol::Error::BufferUnderflow) => break,
                Err(err) => return Err(err),
            }
        }

        left_sz = read_buffer.len();
        buffer.copy_within(left_sz.., 0);
    }

    Ok(result)
}
