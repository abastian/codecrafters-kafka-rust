use std::collections::HashMap;
use std::string::String as StdString;

use bytes::{BufMut, BytesMut};

use crate::protocol::{
    self,
    r#type::{CompactArray, CompactString, Int16, TaggedField, TaggedFields},
    Readable, Writable,
};

pub struct V3Request {
    client_software_name: StdString,
    client_software_version: StdString,
}
impl Readable for V3Request {
    fn read(buffer: &mut impl bytes::Buf) -> Result<Self, protocol::Error> {
        let client_software_name =
            std::str::from_utf8(CompactString::read(buffer)?.0.as_ref())?.to_owned();
        let client_software_version =
            std::str::from_utf8(CompactString::read(buffer)?.0.as_ref())?.to_owned();
        let _tagged_fields = TaggedFields::read(buffer)?;

        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

pub enum Request {
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
        CompactString({
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
        CompactString({
            let mut data = BytesMut::with_capacity(self.name.len());
            data.put_slice(self.name.as_bytes());
            data.freeze()
        })
        .write(buffer);
        Int16(self.max_version_level).write(buffer);
        Int16(self.min_version_level).write(buffer);
    }
}

pub enum ApiVersions<'a> {
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
impl<'a> ApiVersions<'a> {
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
impl<'a> Writable for ApiVersions<'a> {
    fn write(&self, buffer: &mut impl bytes::BufMut) {
        match self {
            ApiVersions::Error(error_code) => {
                buffer.put_i16(*error_code);
                buffer.put_u8(0); // api_keys
                buffer.put_i32(0); // throttle_time_ms
                buffer.put_u8(0); // empty _tagged_fields
            }
            ApiVersions::Success {
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
    V3(ApiVersions<'a>),
    V4(ApiVersions<'a>),
}
