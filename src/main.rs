#![allow(unused_imports)]
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::{
    protocol::{
        self,
        message::ApiVersions,
        r#type::{NullableString, TaggedFields},
        Readable, Writable,
    },
    FINALIZED_FEATURES, FINALIZED_FEATURES_EPOCH, SUPPORTED_APIS, SUPPORTED_FEATURES,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                if let Err(err) = handle(stream) {
                    println!("error while handle stream: {}", err);
                }
            }
            Err(err) => {
                println!("error while listening: {}", err);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum KafkaError {
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    ProtocolError(#[from] protocol::Error),
}

fn handle(mut stream: TcpStream) -> Result<(), KafkaError> {
    let mut reader = BufReader::new(&mut stream);
    let mut read_buffer = reader.fill_buf().map_err(KafkaError::IOError)?;

    // request consist of three parts,
    // message_sz part
    let message_sz = read_buffer.get_i32() as usize;
    let mut buffer = read_buffer.copy_to_bytes(message_sz);
    reader.consume(message_sz + 4);

    // header part
    // - request_api_key
    let request_api_key = buffer.get_i16();
    // - request_api_version
    let request_api_version = buffer.get_i16();
    // - correlation_id
    let correlation_id = buffer.get_i32();
    // - client_id
    let _client_id = NullableString::read(&mut buffer);
    // - tagged_fields
    let _tagged_fields = TaggedFields::read(&mut buffer);

    let response_data = match request_api_key {
        18 => {
            let supported_apis = &SUPPORTED_APIS;
            let supported_features = &SUPPORTED_FEATURES;
            let finalized_features = &FINALIZED_FEATURES;
            let api_versions = match protocol::message::read_api_versions_request(
                &mut buffer,
                request_api_version,
            ) {
                Ok(_) => ApiVersions::success(
                    supported_apis,
                    0,
                    supported_features,
                    *FINALIZED_FEATURES_EPOCH,
                    finalized_features,
                    false,
                ),
                Err(err) => match err {
                    protocol::Error::UnsupportedVersion => ApiVersions::error(35),
                    protocol::Error::IllegalArgument(_) | protocol::Error::Utf8Error(_) => {
                        ApiVersions::error(2)
                    }
                },
            };

            let mut buffer = BytesMut::with_capacity(16);
            api_versions.write(&mut buffer);
            buffer.freeze()
        }
        _ => Bytes::new(),
    };

    let message_size = response_data.len() as i32 + 4;

    let header_data = {
        let mut data = BytesMut::with_capacity(8);
        data.put_i32(message_size);
        data.put_i32(correlation_id);
        data.freeze()
    };

    stream
        .write_all(&header_data)
        .map_err(KafkaError::IOError)?;
    stream
        .write_all(&response_data)
        .map_err(KafkaError::IOError)?;
    stream.flush().map_err(KafkaError::IOError)?;

    Ok(())
}
