#![allow(unused_imports)]
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::{
    metadata,
    model::{self, Topic},
    protocol::{
        self,
        message::{process_request, read_request, write_response},
        r#type::TaggedFields,
        Readable, Writable,
    },
};

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn({
                    move || {
                        if let Err(err) = handle(stream) {
                            println!("error while handle stream: {}", err);
                        }
                    }
                });
            }
            Err(err) => {
                println!("error while listening: {}", err);
            }
        }
    }

    Ok(())
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

        let (request_header, request) = read_request(&mut read_buffer)?;
        if read_buffer.is_empty() {
            stream_buffer.clear();
        } else {
            let read_buffer_remaining = read_buffer.remaining();
            let lower_index = stream_buffer.len() - read_buffer_remaining;
            stream_buffer.copy_within(lower_index.., 0);
            stream_buffer.truncate(read_buffer_remaining);
        }

        let response = process_request(request)?;

        let response_data = {
            let mut data = BytesMut::with_capacity(64);
            write_response(&mut data, request_header, response)?;
            data.freeze()
        };

        let message_size = response_data.len() as i32;

        let header_data = {
            let mut data = BytesMut::with_capacity(4);
            message_size.write(&mut data);
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
