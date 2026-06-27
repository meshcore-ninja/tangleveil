# Tangleveil

A small live WebSocket relay for multiple CoreScope instances.

## Behavior

- Connects to every configured upstream WebSocket.
- Reconnects automatically with exponential backoff.
- `GET /ws/{source}` forwards text and binary data frames from one source unchanged.
- `GET /ws` combines all sources using a compact binary envelope.
- Uses bounded Tokio broadcast channels.
- Disconnects downstream clients that cannot keep up.
- Does no packet parsing, deduplication, filtering, or persistence.

## Run

```bash
cp config.example.toml config.toml
cp sources.example.toml sources.toml
# Edit config.toml and sources.toml
RUST_LOG=tangleveil=info cargo run --release -- config.toml
```

Upstream sources live in `sources_file` (default: `sources.toml`):

```toml
[[sources]]
id = "prague"
url = "wss://corescope-prague.example/ws"
```

Endpoints:

```text
GET /health
GET /sources
WS  /ws
WS  /ws/{source}
```

Example source-specific connection:

```js
const ws = new WebSocket("ws://localhost:8080/ws/prague");
ws.onmessage = (event) => console.log(event.data);
```

## Multiplexed envelope

`/ws` always sends binary WebSocket messages. Each message is:

```text
magic          4 bytes   ASCII "CSR1"
kind           1 byte    1=text, 2=binary
source_length  2 bytes   unsigned, big-endian
sequence       8 bytes   unsigned, big-endian, per source
received_at    8 bytes   Unix milliseconds, big-endian
payload_length 4 bytes   unsigned, big-endian
source         N bytes   UTF-8
payload        N bytes   original CoreScope payload
```

Browser decoder:

```js
function decodeRelayFrame(arrayBuffer) {
  const bytes = new Uint8Array(arrayBuffer);
  const view = new DataView(arrayBuffer);
  const decoder = new TextDecoder();

  if (decoder.decode(bytes.subarray(0, 4)) !== "CSR1") {
    throw new Error("Unknown relay frame");
  }

  const kind = view.getUint8(4);
  const sourceLength = view.getUint16(5, false);
  const sequence = view.getBigUint64(7, false);
  const receivedAtMs = view.getBigUint64(15, false);
  const payloadLength = view.getUint32(23, false);
  const sourceStart = 27;
  const payloadStart = sourceStart + sourceLength;

  if (payloadStart + payloadLength !== bytes.length) {
    throw new Error("Invalid relay frame length");
  }

  const source = decoder.decode(bytes.subarray(sourceStart, payloadStart));
  const payloadBytes = bytes.subarray(payloadStart);
  const payload = kind === 1 ? decoder.decode(payloadBytes) : payloadBytes;

  return { kind, source, sequence, receivedAtMs, payload };
}

const ws = new WebSocket("ws://localhost:8080/ws");
ws.binaryType = "arraybuffer";
ws.onmessage = (event) => console.log(decodeRelayFrame(event.data));
```

## Performance characteristics

The source-specific endpoint forwards the upstream text/binary value through a bounded broadcast channel. The multiplexed endpoint creates one envelope per incoming frame; all connected clients share that immutable `Bytes` allocation.

`tokio::sync::broadcast` drops the oldest retained entries when a receiver lags. Tangleveil detects that condition and disconnects the slow client instead of silently continuing with a gap.

## Natural next steps

- subscription/filter control messages
- an in-memory replay ring
- append-only persistent segments or NATS JetStream
- shared dedupe/filter pipelines
- Prometheus metrics
- authentication and per-source authorization
