#![allow(clippy::too_many_arguments)]
use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::{
    self,
    r#type::{Array, CompactArray, CompactKafkaString, TaggedField, TaggedFields},
    Readable, ReadableVersion, Writable,
};

pub(crate) const API_KEY: i16 = 18;

#[derive(Debug, Clone)]
pub struct Request {
    version: i16,
    client_software_name: Option<Bytes>,
    client_software_version: Option<Bytes>,
}
impl Request {
    fn new(
        version: i16,
        client_software_name: Option<&[u8]>,
        client_software_version: Option<&[u8]>,
    ) -> Self {
        Request {
            version,
            client_software_name: client_software_name.map(Bytes::copy_from_slice),
            client_software_version: client_software_version.map(Bytes::copy_from_slice),
        }
    }

    pub fn v0() -> Self {
        Self::new(0, None, None)
    }

    pub fn v1() -> Self {
        Self::new(1, None, None)
    }

    pub fn v2() -> Self {
        Self::new(2, None, None)
    }

    pub fn v3(client_software_name: &str, client_software_version: &str) -> Self {
        Self::new(
            3,
            Some(client_software_name.as_bytes()),
            Some(client_software_version.as_bytes()),
        )
    }

    pub fn v4(client_software_name: &str, client_software_version: &str) -> Self {
        Self::new(
            4,
            Some(client_software_name.as_bytes()),
            Some(client_software_version.as_bytes()),
        )
    }

    pub fn client_software_name(&self) -> Option<&[u8]> {
        self.client_software_name.as_deref()
    }

    pub fn client_software_version(&self) -> Option<&[u8]> {
        self.client_software_version.as_deref()
    }
}
impl ReadableVersion for Request {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        let (client_software_name, client_software_version) = if (3..=4).contains(&version) {
            let client_software_name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument(
                    "non-nullable field clientSoftwareName was serialized as null",
                ),
            )?;
            let client_software_version = CompactKafkaString::read_result_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument(
                    "non-nullable field clientSoftwareVersion was serialized as null",
                ),
            )?;
            (Some(client_software_name), Some(client_software_version))
        } else {
            (None, None)
        };
        if (3..=4).contains(&version) {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }

        Ok(Self {
            version,
            client_software_name,
            client_software_version,
        })
    }
}
impl Writable for Request {
    fn write(&self, buffer: &mut impl BufMut) {
        if self.version >= 3 {
            CompactKafkaString::write_inner(buffer, self.client_software_name());
            CompactKafkaString::write_inner(buffer, self.client_software_version());
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SupportedFeature {
    name: Bytes,
    min_version: i16,
    max_version: i16,
}
impl SupportedFeature {
    fn new(name: &[u8], min_version: i16, max_version: i16) -> Self {
        Self {
            name: Bytes::copy_from_slice(name),
            min_version,
            max_version,
        }
    }

    pub fn v3(name: &str, min_version: i16, max_version: i16) -> Self {
        Self::new(name.as_bytes(), min_version, max_version)
    }

    pub fn v4(name: &str, min_version: i16, max_version: i16) -> Self {
        Self::new(name.as_bytes(), min_version, max_version)
    }

    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub fn min_version(&self) -> i16 {
        self.min_version
    }

    pub fn max_version(&self) -> i16 {
        self.max_version
    }
}
impl ReadableVersion for SupportedFeature {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(3..=4).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }
        let name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field name was serialized as null"),
        )?;
        let min_version = i16::read(buffer);
        let max_version = i16::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self {
            name,
            min_version,
            max_version,
        })
    }
}
impl Writable for SupportedFeature {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactKafkaString::write_inner(buffer, Some(self.name()));
        self.min_version.write(buffer);
        self.max_version.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct ApiKey {
    version: i16,
    api_key: i16,
    min_version: i16,
    max_version: i16,
}
impl ApiKey {
    pub fn v0(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            version: 0,
            api_key,
            min_version,
            max_version,
        }
    }

    pub fn v1(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            version: 1,
            api_key,
            min_version,
            max_version,
        }
    }

    pub fn v2(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            version: 2,
            api_key,
            min_version,
            max_version,
        }
    }

    pub fn v3(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            version: 3,
            api_key,
            min_version,
            max_version,
        }
    }

    pub fn v4(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            version: 4,
            api_key,
            min_version,
            max_version,
        }
    }

    pub fn api_key(&self) -> i16 {
        self.api_key
    }

    pub fn min_version(&self) -> i16 {
        self.min_version
    }

    pub fn max_version(&self) -> i16 {
        self.max_version
    }
}
impl ReadableVersion for ApiKey {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(0..=4).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let api_key = i16::read(buffer);
        let min_version = i16::read(buffer);
        let max_version = i16::read(buffer);
        if version >= 3 {
            let _tagged_fields = TaggedFields::read_result_inner(buffer)?;
        }
        Ok(Self {
            version,
            api_key,
            min_version,
            max_version,
        })
    }
}
impl Writable for ApiKey {
    fn write(&self, buffer: &mut impl BufMut) {
        self.api_key.write(buffer);
        self.min_version.write(buffer);
        self.max_version.write(buffer);
        if self.version >= 3 {
            TaggedFields::write_empty(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FinalizedFeature {
    name: Bytes,
    max_version_level: i16,
    min_version_level: i16,
}
impl FinalizedFeature {
    fn new(name: &[u8], max_version_level: i16, min_version_level: i16) -> Self {
        Self {
            name: Bytes::copy_from_slice(name),
            max_version_level,
            min_version_level,
        }
    }

    pub fn v3(name: &str, max_version_level: i16, min_version_level: i16) -> Self {
        Self::new(name.as_bytes(), max_version_level, min_version_level)
    }

    pub fn v4(name: &str, max_version_level: i16, min_version_level: i16) -> Self {
        Self::new(name.as_bytes(), max_version_level, min_version_level)
    }

    pub fn name(&self) -> &[u8] {
        self.name.as_ref()
    }

    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    pub fn min_version_level(&self) -> i16 {
        self.min_version_level
    }
}
impl ReadableVersion for FinalizedFeature {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(3..=4).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let name = CompactKafkaString::read_result_inner(buffer)?.ok_or(
            protocol::Error::IllegalArgument("non-nullable field name was serialized as null"),
        )?;
        let max_version_level = i16::read(buffer);
        let min_version_level = i16::read(buffer);
        let _tagged_fields = TaggedFields::read_result_inner(buffer)?;

        Ok(Self {
            name,
            max_version_level,
            min_version_level,
        })
    }
}
impl Writable for FinalizedFeature {
    fn write(&self, buffer: &mut impl BufMut) {
        CompactKafkaString::write_inner(buffer, Some(self.name()));
        self.max_version_level.write(buffer);
        self.min_version_level.write(buffer);
        TaggedFields::write_empty(buffer);
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub(crate) version: i16,
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: Option<i32>,
    supported_features: Option<Vec<SupportedFeature>>,
    finalized_features_epoch: Option<i64>,
    finalized_features: Option<Vec<FinalizedFeature>>,
    zk_migration_ready: Option<bool>,
}
impl Response {
    fn new(
        version: i16,
        error_code: i16,
        api_keys: Vec<ApiKey>,
        throttle_time_ms: Option<i32>,
        supported_features: Option<Vec<SupportedFeature>>,
        finalized_features_epoch: Option<i64>,
        finalized_features: Option<Vec<FinalizedFeature>>,
        zk_migration_ready: Option<bool>,
    ) -> Self {
        Self {
            version,
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        }
    }

    pub fn v0(error_code: i16, api_keys: &HashMap<i16, ApiKey>) -> Self {
        Self::new(
            0,
            error_code,
            api_keys.values().cloned().collect(),
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub fn v1(error_code: i16, api_keys: &HashMap<i16, ApiKey>, throttle_time_ms: i32) -> Self {
        Self::new(
            1,
            error_code,
            api_keys.values().cloned().collect(),
            Some(throttle_time_ms),
            None,
            None,
            None,
            None,
        )
    }

    pub fn v2(error_code: i16, api_keys: &HashMap<i16, ApiKey>, throttle_time_ms: i32) -> Self {
        Self::new(
            2,
            error_code,
            api_keys.values().cloned().collect(),
            Some(throttle_time_ms),
            None,
            None,
            None,
            None,
        )
    }

    pub fn v3(
        error_code: i16,
        api_keys: &HashMap<i16, ApiKey>,
        throttle_time_ms: i32,
        supported_features: Option<&HashMap<String, SupportedFeature>>,
        finalized_features_epoch: Option<i64>,
        finalized_features: Option<&HashMap<String, FinalizedFeature>>,
        zk_migration_ready: Option<bool>,
    ) -> Self {
        Self::new(
            3,
            error_code,
            api_keys.values().cloned().collect(),
            Some(throttle_time_ms),
            supported_features.map(|h| h.values().cloned().collect()),
            finalized_features_epoch,
            finalized_features.map(|h| h.values().cloned().collect()),
            zk_migration_ready,
        )
    }

    pub fn v4(
        error_code: i16,
        api_keys: &HashMap<i16, ApiKey>,
        throttle_time_ms: i32,
        supported_features: Option<&HashMap<String, SupportedFeature>>,
        finalized_features_epoch: Option<i64>,
        finalized_features: Option<&HashMap<String, FinalizedFeature>>,
        zk_migration_ready: Option<bool>,
    ) -> Self {
        Self::new(
            4,
            error_code,
            api_keys.values().cloned().collect(),
            Some(throttle_time_ms),
            supported_features.map(|h| h.values().cloned().collect()),
            finalized_features_epoch,
            finalized_features.map(|h| h.values().cloned().collect()),
            zk_migration_ready,
        )
    }

    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    pub fn api_keys(&self) -> &[ApiKey] {
        &self.api_keys
    }

    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms.unwrap_or_default()
    }

    pub fn supported_features(&self) -> Option<&[SupportedFeature]> {
        self.supported_features.as_deref()
    }

    pub fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    pub fn finalized_features(&self) -> Option<&[FinalizedFeature]> {
        self.finalized_features.as_deref()
    }

    pub fn zk_migration_ready(&self) -> Option<bool> {
        self.zk_migration_ready
    }
}
impl ReadableVersion for Response {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if !(0..=4).contains(&version) {
            return Err(protocol::Error::UnsupportedVersion);
        }

        let error_code = i16::read(buffer);
        let api_keys = if version >= 3 {
            CompactArray::<ApiKey>::read_version_inner(buffer, version)
        } else {
            Array::<ApiKey>::read_version_inner(buffer, version)
        }?
        .ok_or(protocol::Error::IllegalArgument(
            "non-nullable field apiKeys was serialized as null",
        ))?;
        let throttle_time_ms = if version >= 1 {
            Some(i32::read(buffer))
        } else {
            None
        };

        let mut supported_features = None;
        let mut finalized_features_epoch = None;
        let mut finalized_features = None;
        let mut zk_migration_ready = None;
        if version >= 3 {
            let tagged_fields = TaggedFields::read_result_inner(buffer)?;

            for tf in tagged_fields {
                let mut inner_buffer = tf.data;
                match tf.key {
                    0 => {
                        let value = CompactArray::<SupportedFeature>::read_version_inner(
                            &mut inner_buffer,
                            version,
                        )?
                        .ok_or(protocol::Error::IllegalArgument(
                            "non-nullable field supportedFeatures was serialized as null",
                        ))?;
                        supported_features.replace(value);
                    }
                    1 => {
                        finalized_features_epoch.replace(i64::read(buffer));
                    }
                    2 => {
                        let value = CompactArray::<FinalizedFeature>::read_version_inner(
                            &mut inner_buffer,
                            version,
                        )?
                        .ok_or(protocol::Error::IllegalArgument(
                            "non-nullable field finalizedFeatures was serialized as null",
                        ))?;
                        finalized_features.replace(value);
                    }
                    3 => {
                        zk_migration_ready.replace(bool::read(buffer));
                    }
                    _ => continue,
                }
            }
        };

        Ok(Self {
            version,
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        })
    }
}
impl Writable for Response {
    fn write(&self, buffer: &mut impl BufMut) {
        self.error_code.write(buffer);
        if self.version >= 3 {
            CompactArray::write_inner(buffer, Some(self.api_keys()));
        } else {
            Array::write_inner(buffer, Some(self.api_keys()));
        }
        if self.version >= 1 {
            self.throttle_time_ms().write(buffer);
        }
        if self.version >= 3 {
            let tagged_fields = {
                let mut value = Vec::new();
                if self.supported_features().is_some() {
                    let mut inner_buffer = BytesMut::with_capacity(16);
                    CompactArray::write_inner(&mut inner_buffer, self.supported_features());
                    value.push(TaggedField::new(0, inner_buffer.freeze()));
                }
                if let Some(finalized_features_epoch) = self.finalized_features_epoch() {
                    let mut inner_buffer = BytesMut::with_capacity(8);
                    finalized_features_epoch.write(&mut inner_buffer);
                    value.push(TaggedField::new(1, inner_buffer.freeze()));
                }
                if self.finalized_features().is_some() {
                    let mut inner_buffer = BytesMut::with_capacity(16);
                    CompactArray::write_inner(&mut inner_buffer, self.finalized_features());
                    value.push(TaggedField::new(2, inner_buffer.freeze()));
                }
                if let Some(zk_migration_ready) = self.zk_migration_ready() {
                    let mut inner_buffer = BytesMut::with_capacity(1);
                    zk_migration_ready.write(&mut inner_buffer);
                    value.push(TaggedField::new(3, inner_buffer.freeze()));
                }

                value
            };
            TaggedFields::write_inner(buffer, &tagged_fields);
        }
    }
}

pub(crate) fn process_request(
    request: Request,
    api_keys: &HashMap<i16, ApiKey>,
    _supported_features: &HashMap<String, SupportedFeature>,
    _finalized_features: &HashMap<String, FinalizedFeature>,
) -> Response {
    if request.version < 0 || request.version > 4 {
        return Response::v0(35, api_keys);
    }

    match request.version {
        0 => Response::v0(0, api_keys),
        1 => Response::v1(0, api_keys, 0),
        2 => Response::v2(0, api_keys, 0),
        3 => Response::v3(0, api_keys, 0, None, None, None, None),
        4 => Response::v4(0, api_keys, 0, None, None, None, None),
        _ => unreachable!(),
    }
}
