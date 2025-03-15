pub(crate) mod api_versions;

pub use api_versions::ApiVersions;
use bytes::Buf;

use super::Readable;

pub fn read_api_versions_request(
    buffer: &mut impl Buf,
    version: i16,
) -> Result<api_versions::Request, super::Error> {
    match version {
        3 => Ok(api_versions::Request::V3(api_versions::V3Request::read(
            buffer,
        )?)),
        4 => Ok(api_versions::Request::V4(api_versions::V3Request::read(
            buffer,
        )?)),
        _ => Err(super::Error::UnsupportedVersion),
    }
}
