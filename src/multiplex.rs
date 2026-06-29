use std::borrow::Cow;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ws::Utf8Bytes;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

const MULTIPLEX_MAGIC: &[u8; 4] = b"CSR1";
const MULTIPLEX_HEADER_LEN: usize = 27;

/// A multiplex frame shared across all subscribers. Holds the canonical binary
/// envelope plus a lazily-computed, cached JSON projection.
///
/// Broadcast subscribers receive a clone of the same `Arc<MultiplexFrame>`, so
/// the JSON encoding is computed at most once per frame (by the first JSON
/// client to ask) and reused by every other JSON client. Binary clients never
/// touch the JSON field.
pub struct MultiplexFrame {
    binary: Bytes,
    json: OnceLock<Option<Utf8Bytes>>,
    meta: OnceLock<FrameMeta>,
}

/// Cheap, filter-relevant fields pulled out of the embedded CoreScope packet.
/// Extracted with a single JSON parse the first time any client needs either
/// field, then shared by every subscriber — so per-client filtering is just a
/// set lookup, never a re-parse.
#[derive(Default)]
struct FrameMeta {
    /// MeshCore payload type, upper-cased (e.g. `"ADVERT"`, `"REQ"`).
    payload_type: Option<Box<str>>,
    /// CoreScope's per-packet content hash, stable across observers/sources.
    content_hash: Option<Box<str>>,
}

impl MultiplexFrame {
    pub fn new(binary: Bytes) -> Self {
        Self {
            binary,
            json: OnceLock::new(),
            meta: OnceLock::new(),
        }
    }

    /// The raw CSR1 binary envelope. Cloning is a cheap refcount bump.
    pub fn binary(&self) -> Bytes {
        self.binary.clone()
    }

    /// Source id parsed from the envelope, used for filtering.
    pub fn source_id(&self) -> Option<&str> {
        source_id_from_multiplex_frame(&self.binary)
    }

    /// Filter metadata for the embedded packet, parsed once and shared across
    /// all subscribers (see [`FrameMeta`]).
    fn meta(&self) -> &FrameMeta {
        self.meta.get_or_init(|| frame_meta(&self.binary))
    }

    /// MeshCore payload type of the embedded CoreScope packet (e.g. `"ADVERT"`,
    /// `"REQ"`, `"TXT_MSG"`), used for `?payloadTypes=` filtering. Upper-cased
    /// so matching is case-insensitive without per-client allocation. `None` for
    /// frames without a recognizable type (non-`packet` messages, binary/base64
    /// payloads, or malformed JSON).
    pub fn payload_type(&self) -> Option<&str> {
        self.meta().payload_type.as_deref()
    }

    /// CoreScope's content hash for the embedded packet, used for
    /// `?dedupByContent`. The same physical packet seen by many observers (or
    /// relayed through many sources) carries the same hash, so deduping on it
    /// collapses those copies. `None` when the upstream message has no `hash`.
    pub fn content_hash(&self) -> Option<&str> {
        self.meta().content_hash.as_deref()
    }

    /// The JSON projection as a ready-to-send WebSocket text payload. Computed
    /// once and cached; `None` if the frame can't be projected (malformed, or
    /// serialization failed). The cached `Utf8Bytes` clones cheaply.
    pub fn json(&self) -> Option<Utf8Bytes> {
        self.json
            .get_or_init(|| {
                let decoded = decode_multiplex_frame_json(&self.binary)?;
                serde_json::to_string(&decoded).ok().map(Utf8Bytes::from)
            })
            .clone()
    }
}

pub fn source_id_from_multiplex_frame(frame: &[u8]) -> Option<&str> {
    if frame.len() < MULTIPLEX_HEADER_LEN || frame.get(..4)? != MULTIPLEX_MAGIC {
        return None;
    }

    let source_len = u16::from_be_bytes([frame[5], frame[6]]) as usize;
    let end = MULTIPLEX_HEADER_LEN.checked_add(source_len)?;
    let source_bytes = frame.get(MULTIPLEX_HEADER_LEN..end)?;
    std::str::from_utf8(source_bytes).ok()
}

/// The raw payload bytes of a CSR1 frame (the upstream CoreScope message body),
/// without copying. `None` for non-text frames or truncated/malformed envelopes.
fn payload_bytes_from_multiplex_frame(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < MULTIPLEX_HEADER_LEN || frame.get(..4)? != MULTIPLEX_MAGIC {
        return None;
    }
    // Only text frames (kind 1) carry the JSON we extract a payload type from.
    if frame[4] != 1 {
        return None;
    }
    let source_len = u16::from_be_bytes([frame[5], frame[6]]) as usize;
    let payload_len = u32::from_be_bytes(frame[23..27].try_into().ok()?) as usize;
    let source_end = MULTIPLEX_HEADER_LEN.checked_add(source_len)?;
    let payload_end = source_end.checked_add(payload_len)?;
    frame.get(source_end..payload_end)
}

/// Pull the filter-relevant fields out of a CoreScope `packet` message in a
/// single parse. CoreScope nests the payload type at
/// `data.decoded.header.payloadTypeName` and the content hash at `data.hash`;
/// every field is optional so non-packet messages (e.g. status frames) simply
/// yield an empty [`FrameMeta`] instead of failing.
fn frame_meta(frame: &[u8]) -> FrameMeta {
    let Some(payload) = payload_bytes_from_multiplex_frame(frame) else {
        return FrameMeta::default();
    };

    #[derive(Deserialize)]
    struct Probe<'a> {
        #[serde(borrow, default)]
        data: Option<ProbeData<'a>>,
    }
    #[derive(Deserialize)]
    struct ProbeData<'a> {
        #[serde(borrow, default)]
        decoded: Option<ProbeDecoded<'a>>,
        #[serde(borrow, default)]
        hash: Option<Cow<'a, str>>,
    }
    #[derive(Deserialize)]
    struct ProbeDecoded<'a> {
        #[serde(borrow, default)]
        header: Option<ProbeHeader<'a>>,
    }
    #[derive(Deserialize)]
    struct ProbeHeader<'a> {
        #[serde(rename = "payloadTypeName", borrow, default)]
        payload_type_name: Option<Cow<'a, str>>,
    }

    let Ok(probe) = serde_json::from_slice::<Probe>(payload) else {
        return FrameMeta::default();
    };
    let Some(data) = probe.data else {
        return FrameMeta::default();
    };

    let content_hash = data
        .hash
        .as_deref()
        .filter(|h| !h.is_empty())
        .map(Box::<str>::from);
    let payload_type = data
        .decoded
        .and_then(|d| d.header)
        .and_then(|h| h.payload_type_name)
        .map(|name| name.to_ascii_uppercase().into_boxed_str());

    FrameMeta {
        payload_type,
        content_hash,
    }
}

/// JSON projection of a CSR1 multiplex frame. The `encoding` field tells the
/// client how to read `payload`:
///
/// - `"json"`  — the text payload was itself valid JSON, embedded inline as a
///   nested value (no double-escaping).
/// - `"utf8"`  — text payload that isn't valid JSON, carried as a JSON string.
/// - `"base64"` — binary (or non-UTF-8) payload, base64-encoded into a string.
#[derive(Debug, Serialize)]
pub struct MultiplexFrameJson<'a> {
    pub source: &'a str,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: u8,
    pub encoding: &'static str,
    pub payload: PayloadJson<'a>,
}

/// The `payload` field of [`MultiplexFrameJson`]. Untagged so it serializes as
/// either a bare JSON value (`Json`) or a JSON string (`Str`); the sibling
/// `encoding` field disambiguates for consumers.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum PayloadJson<'a> {
    /// Pre-validated JSON, reinserted verbatim without re-escaping.
    Json(&'a RawValue),
    /// Escaped UTF-8 text, or base64 of binary.
    Str(Cow<'a, str>),
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
        Ok(text) if kind == 1 => match serde_json::from_str::<&RawValue>(text) {
            // The text frame is itself valid JSON — embed it verbatim so clients
            // don't have to parse a JSON string out of a JSON string.
            Ok(raw) => ("json", PayloadJson::Json(raw)),
            // Plain text that isn't JSON: carry it as a string.
            Err(_) => ("utf8", PayloadJson::Str(Cow::Borrowed(text))),
        },
        _ => ("base64", PayloadJson::Str(Cow::Owned(STANDARD.encode(payload_bytes)))),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn to_json(kind: u8, payload: &[u8]) -> serde_json::Value {
        let frame = encode_multiplex_frame("src-1", 7, 1_700_000_000_000, kind, payload);
        let decoded = decode_multiplex_frame_json(&frame).expect("decodes");
        let text = serde_json::to_string(&decoded).expect("serializes");
        serde_json::from_str(&text).expect("valid json")
    }

    #[test]
    fn embeds_json_text_payload_inline() {
        let value = to_json(1, br#"{"foo":1,"bar":[2,3]}"#);
        assert_eq!(value["encoding"], "json");
        // Embedded as a nested object, not a string.
        assert_eq!(value["payload"]["foo"], 1);
        assert_eq!(value["payload"]["bar"][1], 3);
        assert_eq!(value["source"], "src-1");
        assert_eq!(value["sequence"], 7);
    }

    #[test]
    fn carries_non_json_text_as_string() {
        let value = to_json(1, b"hello, world");
        assert_eq!(value["encoding"], "utf8");
        assert_eq!(value["payload"], "hello, world");
    }

    #[test]
    fn base64_encodes_binary_payload() {
        let value = to_json(2, &[0xff, 0x00, 0x10]);
        assert_eq!(value["encoding"], "base64");
        assert_eq!(value["payload"], STANDARD.encode([0xff, 0x00, 0x10]));
    }

    #[test]
    fn binary_kind_is_never_embedded_as_json() {
        // Even valid-JSON bytes on a binary frame stay base64.
        let value = to_json(2, br#"{"a":1}"#);
        assert_eq!(value["encoding"], "base64");
    }

    #[test]
    fn rejects_truncated_frame() {
        let frame = encode_multiplex_frame("src-1", 1, 1, 1, b"payload");
        assert!(decode_multiplex_frame_json(&frame[..frame.len() - 2]).is_none());
    }

    fn meta_of(payload: &[u8]) -> FrameMeta {
        let frame = encode_multiplex_frame("src-1", 1, 1, 1, payload);
        frame_meta(&frame)
    }

    #[test]
    fn extracts_and_uppercases_payload_type() {
        let payload = br#"{"type":"packet","data":{"decoded":{"header":{"payloadTypeName":"advert"}}}}"#;
        assert_eq!(meta_of(payload).payload_type.as_deref(), Some("ADVERT"));
    }

    #[test]
    fn extracts_payload_type_and_content_hash_together() {
        let payload = br#"{"type":"packet","data":{"hash":"77ab40bf8e68dd4e","decoded":{"header":{"payloadTypeName":"REQ"},"payload":{"type":"REQ"}},"observer_name":"x","id":5}}"#;
        let meta = meta_of(payload);
        assert_eq!(meta.payload_type.as_deref(), Some("REQ"));
        assert_eq!(meta.content_hash.as_deref(), Some("77ab40bf8e68dd4e"));
    }

    #[test]
    fn content_hash_present_without_decoded_payload_type() {
        let meta = meta_of(br#"{"type":"packet","data":{"hash":"abc123"}}"#);
        assert_eq!(meta.content_hash.as_deref(), Some("abc123"));
        assert!(meta.payload_type.is_none());
    }

    #[test]
    fn meta_empty_for_non_packet_message() {
        let meta = meta_of(br#"{"type":"stats","data":{"connected":3}}"#);
        assert!(meta.payload_type.is_none());
        assert!(meta.content_hash.is_none());
        let meta = meta_of(b"not json");
        assert!(meta.payload_type.is_none());
        assert!(meta.content_hash.is_none());
    }

    #[test]
    fn meta_empty_for_binary_frame() {
        let frame = encode_multiplex_frame(
            "src-1",
            1,
            1,
            2,
            br#"{"data":{"hash":"x","decoded":{"header":{"payloadTypeName":"ADVERT"}}}}"#,
        );
        let meta = frame_meta(&frame);
        assert!(meta.payload_type.is_none());
        assert!(meta.content_hash.is_none());
    }
}
