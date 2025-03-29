use bytes::Bytes;
use response::{FetchableTopicResponse, PartitionData};
use uuid::Uuid;

pub(crate) mod request;
pub(crate) mod response;

pub(crate) const API_KEY: i16 = 1;

#[derive(Debug, Clone)]
enum TopicID {
    Name(Bytes),
    Id(Uuid),
}

pub fn process_request(request: request::Request) -> response::Response {
    let version = request.version;
    let responses = request
        .topics()
        .iter()
        .map(|t| {
            FetchableTopicResponse::new(
                version,
                t.topic.clone(),
                vec![PartitionData::new(
                    version, 0, 100, 0, None, None, None, -1, None, None, None, None,
                )],
            )
        })
        .collect();

    response::Response {
        version,
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses,
        node_endpoints: None,
    }
}
