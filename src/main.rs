#![allow(unused_imports)]
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::protocol::{
    self,
    r#type::{NullableKafkaString, TaggedFields},
    Readable, Writable,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| {
                    if let Err(err) = handle(stream) {
                        println!("error while handle stream: {}", err);
                    }
                });
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
    let mut filler = vec![0u8; 1024];
    let mut stream_buffer = Vec::with_capacity(8192);
    loop {
        let read_bytes = stream.read(&mut filler)?;
        if read_bytes == 0 {
            return Ok(());
        }
        stream_buffer.put_slice(&(&filler)[..read_bytes]);

        let mut read_buffer: &[u8] = stream_buffer.as_mut();
        // request consist of three parts,
        // message_sz part
        if read_buffer.remaining() < 4 {
            continue;
        }
        let message_sz = read_buffer.get_i32() as usize;
        if read_buffer.remaining() < message_sz {
            continue;
        }
        let mut buffer = read_buffer.copy_to_bytes(message_sz);
        if read_buffer.is_empty() {
            stream_buffer.clear();
        } else {
            let read_buffer_remaining = read_buffer.remaining();
            let lower_index = stream_buffer.len() - read_buffer_remaining;
            stream_buffer.copy_within(lower_index.., 0);
            stream_buffer.truncate(read_buffer_remaining);
        }

        // header part
        // - request_api_key
        let request_api_key = buffer.get_i16();
        // - request_api_version
        let request_api_version = buffer.get_i16();
        // - correlation_id
        let correlation_id = buffer.get_i32();
        // - client_id
        let _client_id = NullableKafkaString::read(&mut buffer);
        // - tagged_fields
        let _tagged_fields = TaggedFields::read(&mut buffer);

        let response_data = match request_api_key {
            18 => {
                let api_versions = protocol::message::process_api_versions_request(
                    &mut buffer,
                    correlation_id,
                    request_api_version,
                );

                let mut buffer = BytesMut::with_capacity(16);
                api_versions.write(&mut buffer);
                buffer.freeze()
            }
            75 => {
                let describe_topic_partitions =
                    protocol::message::process_describe_topic_partitions_request(
                        &mut buffer,
                        correlation_id,
                        request_api_version,
                    );

                let mut buffer = BytesMut::with_capacity(16);
                describe_topic_partitions.write(&mut buffer);
                buffer.freeze()
            }
            _ => Bytes::new(),
        };

        let message_size = response_data.len() as i32;

        let header_data = {
            let mut data = BytesMut::with_capacity(4);
            data.put_i32(message_size);
            data.freeze()
        };

        stream
            .write_all(&header_data)
            .map_err(KafkaError::IOError)?;
        stream
            .write_all(&response_data)
            .map_err(KafkaError::IOError)?;
        stream.flush().map_err(KafkaError::IOError)?;
    }
}
