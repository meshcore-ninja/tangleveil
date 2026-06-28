use std::fmt;

use serde::Serialize;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Disabled = 0,
    Disconnected = 1,
    Connecting = 2,
    Handshaking = 3,
    Connected = 4,
    ReconnectWait = 5,
    ReconnectWaitDegraded = 6,
    Stopping = 7,
}

impl ConnectionState {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Disabled,
            1 => Self::Disconnected,
            2 => Self::Connecting,
            3 => Self::Handshaking,
            4 => Self::Connected,
            5 => Self::ReconnectWait,
            6 => Self::ReconnectWaitDegraded,
            7 => Self::Stopping,
            _ => Self::Disconnected,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Handshaking => "handshaking",
            Self::Connected => "connected",
            Self::ReconnectWait => "reconnect_wait",
            Self::ReconnectWaitDegraded => "reconnect_wait_degraded",
            Self::Stopping => "stopping",
        }
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ConnectionState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}
