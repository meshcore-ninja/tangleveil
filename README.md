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

## Run with Docker

```bash
cp config.example.toml config.toml
cp sources.example.toml sources.toml
# Edit config.toml and sources.toml.
# Inside the container, bind to 0.0.0.0 so the mapped port is reachable:
sed -i '' 's/^listen = .*/listen = "0.0.0.0:8080"/' config.toml   # macOS/BSD sed

docker compose up --build
```

`docker-compose.yml` builds the image from source by default and bind-mounts
`config.toml`/`sources.toml` from the host so secrets never end up in the image.
To edit the status page without rebuilding, uncomment the `static/` volume in
`docker-compose.yml` and reload it with `POST /admin/reload` or `SIGHUP`.

Upstream sources live in `sources_file` (default: `sources.toml`):

```toml
[[sources]]
id = "prague"
url = "wss://corescope-prague.example/ws"
```

Endpoints:

```text
GET  /health
GET  /sources
GET  /metrics         (Prometheus exposition format)
POST /admin/reload    (requires admin token)
WS   /ws
WS   /ws/{source}
WS   /ws/telemetry
```

Example source-specific connection:

```js
const ws = new WebSocket("ws://localhost:8080/ws/prague");
ws.onmessage = (event) => console.log(event.data);
```

## Configuration reload

Tangleveil can reload `config.toml` and the sources file from disk without restarting the process. On reload it:

- adds, removes, or updates upstream sources
- reconnects sources whose URL or headers changed
- applies reconnect backoff settings from config
- refreshes the admin token

Changes to `listen` are ignored until you restart the process.

### HTTP

Set a strong `admin_token` in `config.toml`. Admin endpoints require a Bearer token. Leave `admin_token` empty or at the default placeholder (`change-me`) to disable the admin API.

```bash
curl -X POST http://127.0.0.1:8080/admin/reload \
  -H "Authorization: Bearer your-secret-token"
```

A successful reload returns `{"status":"reloaded"}`. Invalid or missing tokens return `401 Unauthorized`; a disabled admin API returns `503 Service Unavailable`.

### SIGHUP (Unix)

On Unix systems, sending `SIGHUP` to the process also triggers a reload:

```bash
kill -HUP <pid>
```

This does not require the admin token.

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
- authentication and per-source authorization
