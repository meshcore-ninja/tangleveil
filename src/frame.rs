use axum::extract::ws::{Message, Utf8Bytes};
use bytes::Bytes;

#[derive(Clone)]
pub enum RawFrame {
    Text(Utf8Bytes),
    Binary(Bytes),
}

impl RawFrame {
    pub fn kind(&self) -> u8 {
        match self {
            Self::Text(_) => 1,
            Self::Binary(_) => 2,
        }
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            Self::Text(value) => value.as_bytes(),
            Self::Binary(value) => value.as_ref(),
        }
    }

    pub fn into_axum_message(self) -> Message {
        match self {
            Self::Text(value) => Message::Text(value),
            Self::Binary(value) => Message::Binary(value),
        }
    }
}
