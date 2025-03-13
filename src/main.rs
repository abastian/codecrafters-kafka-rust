#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

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

type CompactArray = Vec<()>;

struct NullableString {
    sz: i16,
    data: Option<bytes::Bytes>,
}

enum RequestHeader {
    RequestHeaderV2 {
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        client_id: NullableString,
        tag_buffer: CompactArray,
    },
}

struct Request {
    message_sz: i32,
    header: RequestHeader,
    body: (),
}

fn handle(mut stream: TcpStream) -> Result<(), KafkaError> {
    let mut reader = BufReader::new(&mut stream);
    let mut read_buffer = reader.fill_buf().map_err(KafkaError::IOError)?;
    let mut consume = 0;

    // request consist of three parts,
    // message_sz part
    let message_sz = read_buffer.get_i32();
    consume += 4;
    println!("message_sz: {}", message_sz);

    // header part
    // - request_api_key
    let request_api_key = read_buffer.get_i16();
    consume += 2;
    // - request_api_version
    let request_api_version = read_buffer.get_i16();
    consume += 2;
    // - correlation_id
    let correlation_id = read_buffer.get_i32();
    consume += 4;
    // - client_id :
    //   + sz
    let client_id_sz = read_buffer.get_i16();
    consume += 2;
    //   + data
    let client_id_data = if client_id_sz == -1 {
        None
    } else {
        let data = read_buffer.copy_to_bytes(client_id_sz as usize);
        consume += client_id_sz as usize;

        Some(data)
    };
    reader.consume(consume);

    let mut buffer = BytesMut::with_capacity(8);

    // message size
    buffer.put_i32(4);

    // header
    // - correlation id
    buffer.put_i32(correlation_id);

    let response = buffer.freeze();
    stream.write_all(&response).map_err(KafkaError::IOError)?;
    stream.flush().map_err(KafkaError::IOError)?;

    Ok(())
}
