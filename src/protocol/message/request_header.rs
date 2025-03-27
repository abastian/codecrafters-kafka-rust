use bytes::Bytes;

use crate::protocol::{
    self,
    message::REQUEST_HEADER_VERSIONS,
    r#type::{KafkaString, TaggedFields},
    Readable, ReadableResult, Writable,
};

pub struct RequestHeader {
    version: u8,
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<Bytes>,
}
impl RequestHeader {
    fn new(
        version: u8,
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        client_id: Option<&[u8]>,
    ) -> Self {
        Self {
            version,
            request_api_key,
            request_api_version,
            correlation_id,
            client_id: client_id.map(Bytes::copy_from_slice),
        }
    }

    pub fn v1(
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Self {
        Self::new(
            1,
            request_api_key,
            request_api_version,
            correlation_id,
            client_id.map(str::as_bytes),
        )
    }

    pub fn v2(
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Self {
        Self::new(
            2,
            request_api_key,
            request_api_version,
            correlation_id,
            client_id.map(str::as_bytes),
        )
    }

    pub fn request_api_key(&self) -> i16 {
        self.request_api_key
    }

    pub fn request_api_version(&self) -> i16 {
        self.request_api_version
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    pub fn client_id(&self) -> Option<&[u8]> {
        self.client_id.as_deref()
    }
}
impl ReadableResult for RequestHeader {
    fn read_result(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let request_api_key = i16::read(buffer);
        let request_api_version = i16::read(buffer);
        let correlation_id = i32::read(buffer);
        let client_id = KafkaString::read_inner(buffer);
        let version = REQUEST_HEADER_VERSIONS
            .get(&(request_api_key, request_api_version))
            .cloned()
            .unwrap_or(1);
        if version >= 2 {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }

        Ok(Self {
            version,
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        })
    }
}
impl Writable for RequestHeader {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        self.request_api_key.write(buffer);
        self.request_api_version.write(buffer);
        self.correlation_id.write(buffer);
        KafkaString::write_inner(buffer, self.client_id());

        if self.version >= 2 {
            TaggedFields::write_empty(buffer);
        }
    }
}
