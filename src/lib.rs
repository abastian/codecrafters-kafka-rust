use std::{collections::HashMap, sync::LazyLock};

use protocol::message::api_versions::{ApiKeyItem, FinalizedFeatureKey, SupportedFeatureKey};

pub mod protocol;

pub static SUPPORTED_APIS: LazyLock<HashMap<i16, ApiKeyItem>> = LazyLock::new(|| {
    let mut res = HashMap::new();
    res.insert(18, ApiKeyItem::new(18, 3, 4));

    res
});

pub static SUPPORTED_FEATURES: LazyLock<HashMap<String, SupportedFeatureKey>> =
    LazyLock::new(HashMap::new);

pub static FINALIZED_FEATURES_EPOCH: LazyLock<Option<i64>> = LazyLock::new(|| None);

pub static FINALIZED_FEATURES: LazyLock<HashMap<String, FinalizedFeatureKey>> =
    LazyLock::new(HashMap::new);
