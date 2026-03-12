use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use std::io;

pub fn encode_grpc_message<M: Message>(msg: &M) -> io::Result<Bytes> {
    let len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(5 + len);

    buf.put_u8(0);
    buf.put_u32(len as u32);

    msg.encode(&mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(buf.freeze())
}

pub fn decode_grpc_message(data: &[u8]) -> io::Result<&[u8]> {
    if data.len() < 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Frame too short",
        ));
    }

    let compressed = data[0];
    if compressed != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Compression not supported",
        ));
    }

    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;

    if data.len() < 5 + len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete message",
        ));
    }

    Ok(&data[5..5 + len])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_empty_frame() {
        let result = decode_grpc_message(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_compressed_frame() {
        let data = [1, 0, 0, 0, 5, 1, 2, 3, 4, 5];
        let result = decode_grpc_message(&data);
        assert!(result.is_err());
    }
}
