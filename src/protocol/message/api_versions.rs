use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    protocol::{
        self,
        r#type::{
            Boolean, CompactArray, CompactKafkaString, Int16, Int32, Int64, TaggedField,
            TaggedFields,
        },
        Readable, Writable,
    },
    FINALIZED_FEATURES, FINALIZED_FEATURES_EPOCH, SUPPORTED_APIS, SUPPORTED_FEATURES,
};

pub struct V0Request;
impl Readable for V0Request {
    fn read(_buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        Ok(V0Request)
    }
}

pub struct V3Request {
    client_software_name: CompactKafkaString,
    client_software_version: CompactKafkaString,
}
impl V3Request {
    pub fn new(client_software_name: &str, client_software_version: &str) -> Self {
        Self {
            client_software_name: client_software_name.into(),
            client_software_version: client_software_version.into(),
        }
    }

    pub fn client_software_name(&self) -> Result<&str, protocol::Error> {
        self.client_software_name.as_str()
    }

    pub fn client_software_version(&self) -> Result<&str, protocol::Error> {
        self.client_software_version.as_str()
    }
}
impl Readable for V3Request {
    fn read(buffer: &mut impl Buf) -> Result<Self, protocol::Error> {
        let client_software_name = CompactKafkaString::read(buffer)?;
        let client_software_version = CompactKafkaString::read(buffer)?;
        let _tagged_fields = TaggedFields::read(buffer)?;

        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

pub struct SupportedFeatureKey {
    name: CompactKafkaString,
    min_version: Int16,
    max_version: Int16,
}
impl SupportedFeatureKey {
    pub fn new(name: &str, min_version: i16, max_version: i16) -> Self {
        Self {
            name: name.into(),
            min_version: min_version.into(),
            max_version: max_version.into(),
        }
    }
}
impl Writable for &SupportedFeatureKey {
    fn write(&self, buffer: &mut impl BufMut) {
        self.name.write(buffer);
        self.min_version.write(buffer);
        self.max_version.write(buffer);
    }
}

pub struct ApiKeyItem {
    api_key: Int16,
    min_version: Int16,
    max_version: Int16,
}
impl ApiKeyItem {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key: api_key.into(),
            min_version: min_version.into(),
            max_version: max_version.into(),
        }
    }
}
impl Writable for &ApiKeyItem {
    fn write(&self, buffer: &mut impl BufMut) {
        self.api_key.write(buffer);
        self.min_version.write(buffer);
        self.max_version.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

pub struct FinalizedFeatureKey {
    name: CompactKafkaString,
    max_version_level: Int16,
    min_version_level: Int16,
}
impl FinalizedFeatureKey {
    pub fn new(name: &str, max_version_level: i16, min_version_level: i16) -> Self {
        Self {
            name: name.into(),
            max_version_level: max_version_level.into(),
            min_version_level: min_version_level.into(),
        }
    }
}
impl Writable for &FinalizedFeatureKey {
    fn write(&self, buffer: &mut impl BufMut) {
        self.name.write(buffer);
        self.max_version_level.write(buffer);
        self.min_version_level.write(buffer);
    }
}

pub struct V0Response<'a> {
    correlation_id: Int32,
    api_keys: Result<&'a HashMap<i16, ApiKeyItem>, Int16>,
}
impl<'a> V0Response<'a> {
    pub fn error(correlation_id: i32, error_code: i16) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            api_keys: Err(error_code.into()),
        }
    }

    pub fn success(correlation_id: i32, api_keys: &'a HashMap<i16, ApiKeyItem>) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            api_keys: Ok(api_keys),
        }
    }
}
impl<'a> Writable for V0Response<'a> {
    fn write(&self, buffer: &mut impl BufMut) {
        self.correlation_id.write(buffer);
        match &self.api_keys {
            Err(error_code) => {
                error_code.write(buffer);
                buffer.put_u8(0); // api_keys
            }
            Ok(api_keys) => {
                Int16::write(buffer, 0); // error_code
                if api_keys.is_empty() {
                    buffer.put_u8(0);
                } else {
                    CompactArray::write(buffer, &api_keys.values().collect::<Vec<_>>());
                }
            }
        }
    }
}

struct V1ResponseData<'a> {
    api_keys: &'a HashMap<i16, ApiKeyItem>,
    throttle_time_ms: Int32,
}

pub struct V1Response<'a> {
    correlation_id: Int32,
    data: Result<V1ResponseData<'a>, Int16>,
}
impl<'a> V1Response<'a> {
    pub fn error(correlation_id: i32, error_code: i16) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            data: Err(error_code.into()),
        }
    }

    pub fn success(
        correlation_id: i32,
        api_keys: &'a HashMap<i16, ApiKeyItem>,
        throttle_time_ms: i32,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            data: Ok(V1ResponseData {
                api_keys,
                throttle_time_ms: throttle_time_ms.into(),
            }),
        }
    }
}
impl<'a> Writable for V1Response<'a> {
    fn write(&self, buffer: &mut impl BufMut) {
        self.correlation_id.write(buffer);
        match &self.data {
            Err(error_code) => {
                error_code.write(buffer);
                buffer.put_u8(0); // api_keys
                buffer.put_i32(0); // throttle_time_ms
            }
            Ok(V1ResponseData {
                api_keys,
                throttle_time_ms,
            }) => {
                buffer.put_i16(0); // error_code
                if api_keys.is_empty() {
                    CompactArray::<ApiKeyItem>::write_empty(buffer);
                } else {
                    CompactArray::write(buffer, &api_keys.values().collect::<Vec<_>>())
                }
                throttle_time_ms.write(buffer);
            }
        }
    }
}

struct V3ResponseData<'a> {
    api_keys: &'a HashMap<i16, ApiKeyItem>,
    throttle_time_ms: Int32,

    supported_features: &'a HashMap<String, SupportedFeatureKey>,
    finalized_features_epoch: Option<Int64>,
    finalized_features: &'a HashMap<String, FinalizedFeatureKey>,
    zk_migration_ready: Boolean,
}

pub struct V3Response<'a> {
    correlation_id: Int32,
    data: Result<V3ResponseData<'a>, Int16>,
}
impl<'a> V3Response<'a> {
    pub fn error(correlation_id: i32, error_code: i16) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            data: Err(error_code.into()),
        }
    }

    pub fn success(
        correlation_id: i32,
        api_keys: &'a HashMap<i16, ApiKeyItem>,
        throttle_time_ms: i32,
        supported_features: &'a HashMap<String, SupportedFeatureKey>,
        finalized_features_epoch: Option<i64>,
        finalized_features: &'a HashMap<String, FinalizedFeatureKey>,
        zk_migration_ready: bool,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            data: Ok(V3ResponseData {
                api_keys,
                throttle_time_ms: throttle_time_ms.into(),
                supported_features,
                finalized_features_epoch: finalized_features_epoch.map(|v| v.into()),
                finalized_features,
                zk_migration_ready: zk_migration_ready.into(),
            }),
        }
    }
}
impl<'a> Writable for V3Response<'a> {
    fn write(&self, buffer: &mut impl BufMut) {
        self.correlation_id.write(buffer);
        match &self.data {
            Err(error_code) => {
                error_code.write(buffer);
                CompactArray::<ApiKeyItem>::write_empty(buffer); // api_keys
                Int32::write(buffer, 0); // throttle_time_ms
                TaggedFields::write_empty(buffer); // empty _tagged_fields
            }
            Ok(V3ResponseData {
                api_keys,
                throttle_time_ms,
                supported_features,
                finalized_features_epoch,
                finalized_features,
                zk_migration_ready,
            }) => {
                buffer.put_i16(0); // error_code
                if api_keys.is_empty() {
                    CompactArray::<ApiKeyItem>::write_empty(buffer);
                } else {
                    CompactArray::write(buffer, &api_keys.values().collect::<Vec<_>>());
                }
                throttle_time_ms.write(buffer);

                let mut tagged_fields = Vec::<TaggedField>::new();
                if !supported_features.is_empty() {
                    let mut buffer = BytesMut::with_capacity(24 * supported_features.len());
                    CompactArray::write(
                        &mut buffer,
                        &supported_features.values().collect::<Vec<_>>(),
                    );
                    let data = buffer.freeze();
                    let tagged_field = TaggedField::new(0, data);
                    tagged_fields.push(tagged_field);
                }
                if let Some(finalized_feature_epoch) = finalized_features_epoch {
                    let mut buffer = BytesMut::with_capacity(8);
                    finalized_feature_epoch.write(&mut buffer);
                    let data = buffer.freeze();
                    let tagged_field = TaggedField::new(1, data);
                    tagged_fields.push(tagged_field);

                    if finalized_feature_epoch.value() > 0 && !finalized_features.is_empty() {
                        let mut buffer = BytesMut::with_capacity(24 * finalized_features.len());
                        CompactArray::write(
                            &mut buffer,
                            &finalized_features.values().collect::<Vec<_>>(),
                        );
                        let data = buffer.freeze();
                        let tagged_field = TaggedField::new(2, data);
                        tagged_fields.push(tagged_field);
                    }

                    if zk_migration_ready.value() {
                        let mut buffer = BytesMut::with_capacity(1);
                        zk_migration_ready.write(&mut buffer);
                        let data = buffer.freeze();
                        let tagged_field = TaggedField::new(3, data);
                        tagged_fields.push(tagged_field);
                    }
                }

                if tagged_fields.is_empty() {
                    TaggedFields::write_empty(buffer);
                } else {
                    TaggedFields::write(buffer, &tagged_fields);
                }
            }
        }
    }
}

pub enum Request {
    V0(V0Request),
    V1(V0Request),
    V2(V0Request),
    V3(V3Request),
    V4(V3Request),
}

pub enum Response<'a> {
    V0(V0Response<'a>),
    V1(V1Response<'a>),
    V2(V1Response<'a>),
    V3(V3Response<'a>),
    V4(V3Response<'a>),
}
impl<'a> Writable for Response<'a> {
    fn write(&self, buffer: &mut impl BufMut) {
        match self {
            Response::V0(resp) => resp.write(buffer),
            Response::V1(resp) => resp.write(buffer),
            Response::V2(resp) => resp.write(buffer),
            Response::V3(resp) => resp.write(buffer),
            Response::V4(resp) => resp.write(buffer),
        }
    }
}

pub fn process_request(buffer: &mut impl Buf, correlation_id: i32, version: i16) -> Response {
    match version {
        0 => {
            if V0Request::read(buffer).is_err() {
                Response::V0(V0Response::error(correlation_id, 2))
            } else {
                Response::V0(V0Response::success(correlation_id, &SUPPORTED_APIS))
            }
        }
        1 => {
            if V0Request::read(buffer).is_err() {
                Response::V1(V1Response::error(correlation_id, 2))
            } else {
                Response::V1(V1Response::success(correlation_id, &SUPPORTED_APIS, 0))
            }
        }
        2 => {
            if V0Request::read(buffer).is_err() {
                Response::V2(V1Response::error(correlation_id, 2))
            } else {
                Response::V2(V1Response::success(correlation_id, &SUPPORTED_APIS, 0))
            }
        }
        3 => {
            if V3Request::read(buffer).is_err() {
                Response::V3(V3Response::error(correlation_id, 2))
            } else {
                Response::V3(V3Response::success(
                    correlation_id,
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
                Response::V4(V3Response::error(correlation_id, 2))
            } else {
                Response::V4(V3Response::success(
                    correlation_id,
                    &SUPPORTED_APIS,
                    0,
                    &SUPPORTED_FEATURES,
                    *FINALIZED_FEATURES_EPOCH,
                    &FINALIZED_FEATURES,
                    false,
                ))
            }
        }
        _ => Response::V0(V0Response::error(correlation_id, 35)),
    }
}
