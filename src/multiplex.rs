use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

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

/// JSON projection of a CSR1 multiplex frame. Text frames (`kind == 1`) carry
/// their UTF-8 payload inline with `encoding == "utf8"`; everything else is
/// base64-encoded with `encoding == "base64"`.
#[derive(Debug, Serialize)]
pub struct MultiplexFrameJson<'a> {
    pub source: &'a str,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: u8,
    pub encoding: &'static str,
    pub payload: String,
}

/// Decode a CSR1 binary envelope into its JSON projection. Returns `None` if the
/// frame is malformed (bad magic, truncated header, or inconsistent lengths).
pub fn decode_multiplex_frame_json(frame: &[u8]) -> Option<MultiplexFrameJson<'_>> {
    if frame.len() < MULTIPLEX_HEADER_LEN || frame.get(..4)? != MULTIPLEX_MAGIC {
        return None;
    }

    let kind = frame[4];
    let source_len = u16::from_be_bytes([frame[5], frame[6]]) as usize;
    let sequence = u64::from_be_bytes(frame[7..15].try_into().ok()?);
    let timestamp_ms = u64::from_be_bytes(frame[15..23].try_into().ok()?);
    let payload_len = u32::from_be_bytes(frame[23..27].try_into().ok()?) as usize;

    let source_end = MULTIPLEX_HEADER_LEN.checked_add(source_len)?;
    let payload_end = source_end.checked_add(payload_len)?;
    if frame.len() < payload_end {
        return None;
    }

    let source = std::str::from_utf8(&frame[MULTIPLEX_HEADER_LEN..source_end]).ok()?;
    let payload_bytes = &frame[source_end..payload_end];

    let (encoding, payload) = match std::str::from_utf8(payload_bytes) {
        Ok(text) if kind == 1 => ("utf8", text.to_owned()),
        _ => ("base64", STANDARD.encode(payload_bytes)),
    };

    Some(MultiplexFrameJson {
        source,
        sequence,
        timestamp_ms,
        kind,
        encoding,
        payload,
    })
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
