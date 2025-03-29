pub(crate) mod api_versions;
pub(crate) mod describe_topic_partitions;
pub(crate) mod fetch;
pub(crate) mod request_header;
pub(crate) mod response_header;

use std::{collections::HashMap, sync::LazyLock};

use bytes::{Buf, BufMut};

use crate::{
    protocol::{ReadableResult, Writable},
    FINALIZED_FEATURES, METADATA_CACHE, SUPPORTED_APIS, SUPPORTED_FEATURES,
};

use super::{Readable, ReadableVersion};

use api_versions::process_request as process_api_versions_request;
use describe_topic_partitions::process_request as process_describe_topic_partitions_request;
use fetch::process_request as process_fetch_request;

pub use api_versions::{Request as ApiVersionsRequest, Response as ApiVersionsResponse};
pub use describe_topic_partitions::{
    Request as DescribeTopicPartitionsRequest, Response as DescribeTopicPartitionsResponse,
};
pub use fetch::{request::Request as FetchRequest, response::Response as FetchResponse};
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;

static REQUEST_HEADER_VERSIONS: LazyLock<HashMap<(i16, i16), u8>> = LazyLock::new(|| {
    HashMap::from([
        ((fetch::API_KEY, 4), 1),
        ((fetch::API_KEY, 5), 1),
        ((fetch::API_KEY, 6), 1),
        ((fetch::API_KEY, 7), 1),
        ((fetch::API_KEY, 8), 1),
        ((fetch::API_KEY, 9), 1),
        ((fetch::API_KEY, 10), 1),
        ((fetch::API_KEY, 11), 1),
        ((fetch::API_KEY, 12), 2),
        ((fetch::API_KEY, 13), 2),
        ((fetch::API_KEY, 14), 2),
        ((fetch::API_KEY, 15), 2),
        ((fetch::API_KEY, 16), 2),
        ((fetch::API_KEY, 17), 2),
        ((api_versions::API_KEY, 0), 1),
        ((api_versions::API_KEY, 1), 1),
        ((api_versions::API_KEY, 2), 1),
        ((api_versions::API_KEY, 3), 2),
        ((api_versions::API_KEY, 4), 2),
        ((describe_topic_partitions::API_KEY, 0), 2),
    ])
});

static RESPONSE_HEADER_VERSIONS: LazyLock<HashMap<(i16, i16), u8>> = LazyLock::new(|| {
    HashMap::from([
        ((fetch::API_KEY, 4), 0),
        ((fetch::API_KEY, 5), 0),
        ((fetch::API_KEY, 6), 0),
        ((fetch::API_KEY, 7), 0),
        ((fetch::API_KEY, 8), 0),
        ((fetch::API_KEY, 9), 0),
        ((fetch::API_KEY, 10), 0),
        ((fetch::API_KEY, 11), 0),
        ((fetch::API_KEY, 12), 1),
        ((fetch::API_KEY, 13), 1),
        ((fetch::API_KEY, 14), 1),
        ((fetch::API_KEY, 15), 1),
        ((fetch::API_KEY, 16), 1),
        ((fetch::API_KEY, 17), 1),
        ((api_versions::API_KEY, 0), 0),
        ((api_versions::API_KEY, 1), 0),
        ((api_versions::API_KEY, 2), 0),
        ((api_versions::API_KEY, 3), 0),
        ((api_versions::API_KEY, 4), 0),
        ((describe_topic_partitions::API_KEY, 0), 1),
    ])
});

pub enum KafkaRequest {
    Fetch(FetchRequest),
    ApiVersions(ApiVersionsRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
}

pub enum KafkaResponse {
    Fetch(FetchResponse),
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

pub fn read_request(buffer: &mut impl Buf) -> Result<(RequestHeader, KafkaRequest), super::Error> {
    if buffer.remaining() < 4 {
        return Err(super::Error::BufferUnderflow);
    }

    let sz = i32::read(buffer) as usize;
    if buffer.remaining() < sz {
        return Err(super::Error::BufferUnderflow);
    }

    let mut inner_buffer = buffer.copy_to_bytes(sz);
    let header = RequestHeader::read_result(&mut inner_buffer)?;

    match header.request_api_key() {
        fetch::API_KEY => {
            let request =
                FetchRequest::read_version(&mut inner_buffer, header.request_api_version())?;
            Ok((header, KafkaRequest::Fetch(request)))
        }
        api_versions::API_KEY => {
            let request =
                ApiVersionsRequest::read_version(&mut inner_buffer, header.request_api_version())?;
            Ok((header, KafkaRequest::ApiVersions(request)))
        }
        describe_topic_partitions::API_KEY => {
            let request = DescribeTopicPartitionsRequest::read_version(
                &mut inner_buffer,
                header.request_api_version(),
            )?;
            Ok((header, KafkaRequest::DescribeTopicPartitions(request)))
        }
        key => Err(super::Error::UnknownRequest(key)),
    }
}

pub fn process_request(request: KafkaRequest) -> Result<KafkaResponse, super::Error> {
    match request {
        KafkaRequest::Fetch(request) => {
            let response = process_fetch_request(request);
            Ok(KafkaResponse::Fetch(response))
        }
        KafkaRequest::ApiVersions(request) => {
            let response = process_api_versions_request(
                request,
                &SUPPORTED_APIS,
                &SUPPORTED_FEATURES,
                &FINALIZED_FEATURES,
            );
            Ok(KafkaResponse::ApiVersions(response))
        }
        KafkaRequest::DescribeTopicPartitions(request) => {
            let response = match METADATA_CACHE.as_ref() {
                Ok(metadata_cache) => {
                    process_describe_topic_partitions_request(request, metadata_cache)?
                }
                Err(err) => return Err(err.clone()),
            };
            Ok(KafkaResponse::DescribeTopicPartitions(response))
        }
    }
}

pub fn write_response(
    buffer: &mut impl BufMut,
    request_header: RequestHeader,
    response: KafkaResponse,
) -> Result<(), super::Error> {
    let api_key = request_header.request_api_key();
    let api_version = request_header.request_api_version();
    let response_header = match RESPONSE_HEADER_VERSIONS.get(&(api_key, api_version)) {
        Some(header_version) => match header_version {
            0 => Ok(ResponseHeader::v0(request_header.correlation_id())),
            1 => Ok(ResponseHeader::v1(request_header.correlation_id())),
            _ => Err(super::Error::UnsupportedVersion),
        },
        None => {
            if api_key == api_versions::API_KEY {
                Ok(ResponseHeader::v0(request_header.correlation_id()))
            } else {
                Err(super::Error::UnsupportedVersion)
            }
        }
    }?;
    response_header.write(buffer);

    match response {
        KafkaResponse::Fetch(resp) => resp.write(buffer),
        KafkaResponse::ApiVersions(resp) => resp.write(buffer),
        KafkaResponse::DescribeTopicPartitions(resp) => resp.write(buffer),
    };

    Ok(())
}
