#![allow(unused_imports)]
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

use bytes::{Buf, BufMut, BytesMut};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum KafkaError {
    #[error("ioerror: {0}")]
    IOError(#[from] std::io::Error),
}

fn handle(mut stream: TcpStream) -> Result<(), KafkaError> {
    let mut buffer = BytesMut::with_capacity(8);

    // message size
    buffer.put_i32(0);

    // header
    // - correlation id
    buffer.put_i32(7);

    let response = buffer.freeze();

    while response.has_remaining() {
        stream.write(&response).map_err(KafkaError::IOError)?;
    }

    Ok(())
}
