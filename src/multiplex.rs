use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};

const MULTIPLEX_MAGIC: &[u8; 4] = b"CSR1";
const MULTIPLEX_HEADER_LEN: usize = 27;

pub fn source_id_from_multiplex_frame(frame: &[u8]) -> Option<&str> {
    if frame.len() < MULTIPLEX_HEADER_LEN || frame.get(..4)? != MULTIPLEX_MAGIC {
        return None;
    }

    let source_len = u16::from_be_bytes([frame[5], frame[6]]) as usize;
    let end = MULTIPLEX_HEADER_LEN.checked_add(source_len)?;
    let source_bytes = frame.get(MULTIPLEX_HEADER_LEN..end)?;
    std::str::from_utf8(source_bytes).ok()
}

pub fn encode_multiplex_frame(
    source: &str,
    sequence: u64,
    received_at_ms: u64,
    kind: u8,
    payload: &[u8],
) -> Bytes {
    // CSR1 binary envelope, all integers big-endian:
    // magic[4], kind[1], source_len[2], sequence[8], timestamp_ms[8], payload_len[4], source, payload
    let source_bytes = source.as_bytes();
    let mut output = BytesMut::with_capacity(27 + source_bytes.len() + payload.len());
    output.extend_from_slice(MULTIPLEX_MAGIC);
    output.put_u8(kind);
    output.put_u16(source_bytes.len() as u16);
    output.put_u64(sequence);
    output.put_u64(received_at_ms);
    output.put_u32(payload.len() as u32);
    output.extend_from_slice(source_bytes);
    output.extend_from_slice(payload);
    output.freeze()
}

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
