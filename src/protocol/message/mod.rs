pub(crate) mod api_versions;
pub(crate) mod describe_topic_partitions;
pub(crate) mod request_header;
pub(crate) mod response_header;

use std::{collections::HashMap, sync::LazyLock};

use crate::{
    protocol::{ReadableResult, Writable},
    FINALIZED_FEATURES, METADATA_CACHE, SUPPORTED_APIS, SUPPORTED_FEATURES,
};

use super::{Readable, ReadableVersion};

use api_versions::process_request as process_api_versions_request;
use describe_topic_partitions::process_request as process_describe_topic_partitions_request;

pub use api_versions::{Request as ApiVersionsRequest, Response as ApiVersionsResponse};
use bytes::{Buf, BufMut};
pub use describe_topic_partitions::{
    Request as DescribeTopicPartitionsRequest, Response as DescribeTopicPartitionsResponse,
};
use request_header::RequestHeader;
use response_header::ResponseHeader;

static REQUEST_HEADER_VERSIONS: LazyLock<HashMap<(i16, i16), u8>> = LazyLock::new(|| {
    HashMap::from([
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
        ((api_versions::API_KEY, 0), 0),
        ((api_versions::API_KEY, 1), 0),
        ((api_versions::API_KEY, 2), 0),
        ((api_versions::API_KEY, 3), 0),
        ((api_versions::API_KEY, 4), 0),
        ((describe_topic_partitions::API_KEY, 0), 1),
    ])
});

pub enum KafkaRequest {
    ApiVersions(ApiVersionsRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
}

pub enum KafkaResponse {
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
        KafkaResponse::ApiVersions(resp) => resp.write(buffer),
        KafkaResponse::DescribeTopicPartitions(resp) => resp.write(buffer),
    };

    Ok(())
}
