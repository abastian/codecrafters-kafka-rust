use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    protocol::{
        self,
        r#type::{CompactArray, CompactKafkaString, Int16, TaggedField, TaggedFields},
        Readable, Writable,
    },
    FINALIZED_FEATURES, FINALIZED_FEATURES_EPOCH, SUPPORTED_APIS, SUPPORTED_FEATURES,
};

pub struct V0Request;
impl Readable for V0Request {
    fn read(_buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        Ok(V0Request)
    }
}

pub struct V3Request {
    client_software_name: String,
    client_software_version: String,
}
impl Readable for V3Request {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let client_software_name =
            std::str::from_utf8(CompactKafkaString::read(buffer)?.0.as_ref())?.to_owned();
        let client_software_version =
            std::str::from_utf8(CompactKafkaString::read(buffer)?.0.as_ref())?.to_owned();
        let _tagged_fields = TaggedFields::read(buffer)?;

        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

pub enum Request {
    V0(V0Request),
    V1(V0Request),
    V2(V0Request),
    V3(V3Request),
    V4(V3Request),
}

pub struct SupportedFeatureKey {
    name: String,
    min_version: i16,
    max_version: i16,
}
impl SupportedFeatureKey {
    pub fn new(name: &str, min_version: i16, max_version: i16) -> Self {
        Self {
            name: name.to_owned(),
            min_version,
            max_version,
        }
    }
}
impl Writable for &SupportedFeatureKey {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        CompactKafkaString({
            let mut data = BytesMut::with_capacity(self.name.len());
            data.put_slice(self.name.as_bytes());
            data.freeze()
        })
        .write(buffer);
        Int16(self.min_version).write(buffer);
        Int16(self.max_version).write(buffer);
    }
}

pub struct ApiKeyItem {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}
impl ApiKeyItem {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }
}
impl Writable for &ApiKeyItem {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        buffer.put_i16(self.api_key);
        buffer.put_i16(self.min_version);
        buffer.put_i16(self.max_version);
        buffer.put_u8(0); // empty _tagged_fields
    }
}

pub struct FinalizedFeatureKey {
    name: String,
    max_version_level: i16,
    min_version_level: i16,
}
impl FinalizedFeatureKey {
    pub fn new(name: &str, max_version_level: i16, min_version_level: i16) -> Self {
        Self {
            name: name.to_owned(),
            max_version_level,
            min_version_level,
        }
    }
}
impl Writable for &FinalizedFeatureKey {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        CompactKafkaString({
            let mut data = BytesMut::with_capacity(self.name.len());
            data.put_slice(self.name.as_bytes());
            data.freeze()
        })
        .write(buffer);
        Int16(self.max_version_level).write(buffer);
        Int16(self.min_version_level).write(buffer);
    }
}

pub enum V0Response<'a> {
    Error(i16),
    Success {
        api_keys: &'a HashMap<i16, ApiKeyItem>,
    },
}
impl<'a> V0Response<'a> {
    pub fn error(error_code: i16) -> Self {
        Self::Error(error_code)
    }

    pub fn success(api_keys: &'a HashMap<i16, ApiKeyItem>) -> Self {
        Self::Success { api_keys }
    }
}
impl<'a> Writable for V0Response<'a> {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self {
            V0Response::Error(error_code) => {
                buffer.put_i16(*error_code);
                buffer.put_u8(0); // api_keys
            }
            V0Response::Success { api_keys } => {
                buffer.put_i16(0); // error_code
                CompactArray {
                    data: Some(api_keys.values().collect::<Vec<_>>()),
                }
                .write(buffer);
            }
        }
    }
}

pub enum V1Response<'a> {
    Error(i16),
    Success {
        api_keys: &'a HashMap<i16, ApiKeyItem>,
        throttle_time_ms: i32,
    },
}
impl<'a> V1Response<'a> {
    pub fn error(error_code: i16) -> Self {
        Self::Error(error_code)
    }

    pub fn success(api_keys: &'a HashMap<i16, ApiKeyItem>, throttle_time_ms: i32) -> Self {
        Self::Success {
            api_keys,
            throttle_time_ms,
        }
    }
}
impl<'a> Writable for V1Response<'a> {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self {
            V1Response::Error(error_code) => {
                buffer.put_i16(*error_code);
                buffer.put_u8(0); // api_keys
                buffer.put_i32(0); // throttle_time_ms
            }
            V1Response::Success {
                api_keys,
                throttle_time_ms,
            } => {
                buffer.put_i16(0); // error_code
                CompactArray {
                    data: Some(api_keys.values().collect::<Vec<_>>()),
                }
                .write(buffer);
                buffer.put_i32(*throttle_time_ms);
            }
        }
    }
}

pub enum V3Response<'a> {
    Error(i16),
    Success {
        api_keys: &'a HashMap<i16, ApiKeyItem>,
        throttle_time_ms: i32,

        supported_features: &'a HashMap<String, SupportedFeatureKey>,
        finalized_features_epoch: Option<i64>,
        finalized_features: &'a HashMap<String, FinalizedFeatureKey>,
        zk_migration_ready: bool,
    },
}
impl<'a> V3Response<'a> {
    pub fn error(error_code: i16) -> Self {
        Self::Error(error_code)
    }

    pub fn success(
        api_keys: &'a HashMap<i16, ApiKeyItem>,
        throttle_time_ms: i32,
        supported_features: &'a HashMap<String, SupportedFeatureKey>,
        finalized_features_epoch: Option<i64>,
        finalized_features: &'a HashMap<String, FinalizedFeatureKey>,
        zk_migration_ready: bool,
    ) -> Self {
        Self::Success {
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        }
    }
}
impl<'a> Writable for V3Response<'a> {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self {
            V3Response::Error(error_code) => {
                buffer.put_i16(*error_code);
                buffer.put_u8(0); // api_keys
                buffer.put_i32(0); // throttle_time_ms
                buffer.put_u8(0); // empty _tagged_fields
            }
            V3Response::Success {
                api_keys,
                throttle_time_ms,
                supported_features,
                finalized_features_epoch,
                finalized_features,
                zk_migration_ready,
            } => {
                buffer.put_i16(0); // error_code
                CompactArray {
                    data: Some(api_keys.values().collect::<Vec<_>>()),
                }
                .write(buffer);
                buffer.put_i32(*throttle_time_ms);

                let mut tagged_fields = TaggedFields::new();
                if !supported_features.is_empty() {
                    let mut buffer = BytesMut::with_capacity(24 * supported_features.len());
                    CompactArray {
                        data: {
                            let mut data = supported_features.values().collect::<Vec<_>>();
                            data.sort_by_key(|sf| &sf.name);
                            Some(data)
                        },
                    }
                    .write(&mut buffer);
                    let data = buffer.freeze();
                    let tagged_field = TaggedField { key: 0, data };
                    tagged_fields.push(tagged_field);
                }
                if let Some(finalized_feature_epoch) = finalized_features_epoch {
                    let mut buffer = BytesMut::with_capacity(8);
                    buffer.put_i64(*finalized_feature_epoch); // Int64(*finalized_feature_epoch).write(&mut buffer);
                    let data = buffer.freeze();
                    let tagged_field = TaggedField { key: 1, data };
                    tagged_fields.push(tagged_field);

                    if *finalized_feature_epoch > 0 && !finalized_features.is_empty() {
                        let mut buffer = BytesMut::with_capacity(24 * finalized_features.len());
                        CompactArray {
                            data: {
                                let mut data = finalized_features.values().collect::<Vec<_>>();
                                data.sort_by_key(|sf| &sf.name);
                                Some(data)
                            },
                        }
                        .write(&mut buffer);
                        let data = buffer.freeze();
                        let tagged_field = TaggedField { key: 2, data };
                        tagged_fields.push(tagged_field);
                    }

                    if *zk_migration_ready {
                        let mut buffer = BytesMut::with_capacity(1);
                        buffer.put_u8(0); // Boolean(true).write(&mut buffer);
                        let data = buffer.freeze();
                        let tagged_field = TaggedField { key: 3, data };
                        tagged_fields.push(tagged_field);
                    }
                }

                tagged_fields.write(buffer);
            }
        }
    }
}

pub enum Response<'a> {
    V0(V0Response<'a>),
    V1(V1Response<'a>),
    V2(V1Response<'a>),
    V3(V3Response<'a>),
    V4(V3Response<'a>),
}
impl<'a> Writable for Response<'a> {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self {
            Response::V0(resp) => resp.write(buffer),
            Response::V1(resp) => resp.write(buffer),
            Response::V2(resp) => resp.write(buffer),
            Response::V3(resp) => resp.write(buffer),
            Response::V4(resp) => resp.write(buffer),
        }
    }
}

pub fn process_api_versions_request(buffer: &mut impl Buf, version: i16) -> Response {
    match version {
        0 => {
            if V0Request::read(buffer).is_err() {
                Response::V0(V0Response::error(2))
            } else {
                Response::V0(V0Response::success(&SUPPORTED_APIS))
            }
        }
        1 => {
            if V0Request::read(buffer).is_err() {
                Response::V1(V1Response::error(2))
            } else {
                Response::V1(V1Response::success(&SUPPORTED_APIS, 0))
            }
        }
        2 => {
            if V0Request::read(buffer).is_err() {
                Response::V2(V1Response::error(2))
            } else {
                Response::V2(V1Response::success(&SUPPORTED_APIS, 0))
            }
        }
        3 => {
            if V3Request::read(buffer).is_err() {
                Response::V3(V3Response::error(2))
            } else {
                Response::V3(V3Response::success(
                    &SUPPORTED_APIS,
                    0,
                    &SUPPORTED_FEATURES,
                    *FINALIZED_FEATURES_EPOCH,
                    &FINALIZED_FEATURES,
                    false,
                ))
            }
        }
        4 => {
            if V3Request::read(buffer).is_err() {
                Response::V4(V3Response::error(2))
            } else {
                Response::V4(V3Response::success(
                    &SUPPORTED_APIS,
                    0,
                    &SUPPORTED_FEATURES,
                    *FINALIZED_FEATURES_EPOCH,
                    &FINALIZED_FEATURES,
                    false,
                ))
            }
        }
        _ => Response::V0(V0Response::error(35)),
    }
}
