use std::collections::HashMap;

use bytes::Bytes;
use response::{FetchableTopicResponse, PartitionData};
use uuid::Uuid;

use crate::{model, protocol};

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
            Some(topic_metadata) => FetchableTopicResponse::new(
                version,
                topic_req.topic.clone(),
                topic_metadata
                    .partitions()
                    .iter()
                    .map(|p| {
                        PartitionData::new(
                            version,
                            p.id(),
                            0,
                            0,
                            None,
                            None,
                            None,
                            -1,
                            None,
                            None,
                            None,
                            None,
                        )
                    })
                    .collect(),
            ),
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
