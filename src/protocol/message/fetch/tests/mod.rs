use bytes::Buf;
use protocol::message::RequestHeader;

use crate::{
    metadata::{Record, RecordBatch},
    protocol::{message::ResponseHeader, Readable, ReadableVersion},
};

use super::{request::Request, response::Response, *};

#[test]
fn test_read_fetch_multi_partition_request() {
    #[rustfmt::skip]
    let data: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x60, 0x00, 0x01, 0x00, 0x10, 0x4c, 0x1a, 0x89, 0x27, 0x00, 0x09, 0x6b, 0x61,
        0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x00, 0x00, 0x00, 0x01, 0xf4, 0x00, 0x00, 0x00, 0x01,
        0x03, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x02, 0x00,
        0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x10, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x01, 0x00,
    ];

    let data_ref: &[u8] = data.as_ref();
    let mut read_buffer = data_ref;

    let sz_request = i32::read(&mut read_buffer);
    assert_eq!(sz_request as usize, read_buffer.remaining());
    let header = RequestHeader::read_result(&mut read_buffer).unwrap();
    assert_eq!(super::API_KEY, header.request_api_key());
    assert_eq!(16, header.request_api_version());
    let _req = Request::read_version(&mut read_buffer, header.request_api_version()).unwrap();
}

#[test]
fn test_read_fetch_multi_partition_response() {
    #[rustfmt::skip]
    let data: Vec<u8> = vec![
        0x00, 0x00, 0x01, 0x15, 0x76, 0x3b, 0x25, 0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x30, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0x00, 0xff, 0xff, 0xff, 0xff, 0xa8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00, 0x00, 0x02, 0xab, 0xfd, 0x04, 0x91, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x6d, 0x8b, 0x00, 0x00, 0x01,
        0x91, 0xe0, 0x5b, 0x6d, 0x8b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x24, 0x00, 0x00, 0x00, 0x01, 0x18, 0x48, 0x65, 0x6c,
        0x6c, 0x6f, 0x20, 0x4b, 0x61, 0x66, 0x6b, 0x61, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x10, 0x00, 0x00, 0x00, 0x4b, 0x00, 0x00, 0x00, 0x00, 0x02, 0x55, 0x60, 0x53, 0x93, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x6d, 0x8b, 0x00, 0x00, 0x01,
        0x91, 0xe0, 0x5b, 0x6d, 0x8b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x32, 0x00, 0x00, 0x00, 0x01, 0x26, 0x48, 0x65, 0x6c,
        0x6c, 0x6f, 0x20, 0x43, 0x6f, 0x64, 0x65, 0x43, 0x72, 0x61, 0x66, 0x74, 0x65, 0x72, 0x73, 0x21,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
    ];

    let data_ref: &[u8] = data.as_ref();
    let mut read_buffer = data_ref;

    let sz_request = i32::read(&mut read_buffer);
    assert_eq!(sz_request as usize, read_buffer.remaining());
    let _header = ResponseHeader::read_version(&mut read_buffer, 1).unwrap();
    let resp = Response::read_version(&mut read_buffer, 16).unwrap();
    assert_eq!(1, resp.responses().len(), "responses len");
    assert_eq!(
        2,
        resp.responses[0].partitions().len(),
        "partitions len of responses[0]"
    );
    {
        let mut records = resp.responses()[0].partitions()[0]
            .records()
            .unwrap()
            .clone();
        let mut values = Vec::with_capacity(2);
        while records.has_remaining() {
            let rb = RecordBatch::read_result(&mut records).unwrap();
            values.push(rb);
        }

        assert_eq!(
            2,
            values.len(),
            "record-batches len of responses[0] partitions[0]"
        );
        assert_eq!(
            1,
            values[0].records().len(),
            "records len of responses[0] partitions[0] record-batches[0]"
        );
        if let Record::Value(record_value) = &values[0].records()[0] {
            assert_eq!(
                "Hello Kafka!",
                std::str::from_utf8(record_value.value().as_ref()).unwrap(),
                "value[0] of responses[0] partitions[0] record-batches[0] records[0]"
            )
        } else {
            assert!(
                false,
                "responses[0] partitions[0] record-batches[0] is not a value-record"
            );
        };

        assert_eq!(
            1,
            values[1].records().len(),
            "records len of responses[0] partitions[0] record-batches[1]"
        );
        if let Record::Value(record_value) = &values[1].records()[0] {
            assert_eq!(
                "Hello CodeCrafters!",
                std::str::from_utf8(record_value.value().as_ref()).unwrap(),
                "value[0] of responses[0] partitions[0] record-batches[1] records[0]"
            )
        } else {
            assert!(
                false,
                "responses[0] partitions[0] record-batches[1] is not a value-record"
            );
        };
    };
    assert_eq!(
        None,
        resp.responses()[0].partitions()[1].records(),
        "responses[0] partitions[0] has empty records"
    );
}
