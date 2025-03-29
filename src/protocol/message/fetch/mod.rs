use bytes::Bytes;
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
    response::Response {
        version: request.version,
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses: vec![],
        node_endpoints: None,
    }
}
