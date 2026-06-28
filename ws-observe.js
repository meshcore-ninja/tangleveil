import { WebSocket } from "k6/websockets";
import { Counter } from "k6/metrics";

const CLIENTS = Number(__ENV.CLIENTS || 100);
const SECONDS = Number(__ENV.SECONDS || 60);
const URL = __ENV.URL;

const connectionsOpened = new Counter("connections_opened");
const connectionErrors = new Counter("connection_errors");
const abnormalCloses = new Counter("abnormal_closes");

export const options = {
  scenarios: {
    websocket_clients: {
      executor: "per-vu-iterations",

      // One VU = one WebSocket client
      vus: CLIENTS,
      iterations: 1,

      maxDuration: `${SECONDS + 30}s`,
    },
  },
};

export default function () {
  if (!URL) {
    throw new Error("URL environment variable is required");
  }

  let opened = false;

  const headers = {};

  if (__ENV.AUTHORIZATION) {
    headers.Authorization = __ENV.AUTHORIZATION;
  }

  const ws = new WebSocket(URL, [], {
    headers,
    tags: {
      test: "websocket-observer",
    },
  });

  ws.binaryType = "arraybuffer";

  ws.addEventListener("open", () => {
    opened = true;
    connectionsOpened.add(1);
  });

  // No parsing or printing: just consume messages.
  ws.addEventListener("message", () => {});

  ws.addEventListener("error", () => {
    connectionErrors.add(1);
  });

  ws.addEventListener("close", (event) => {
    if (!opened || (event.code !== 1000 && event.code !== 1001)) {
      abnormalCloses.add(1);
    }
  });

  setTimeout(() => {
    ws.close();
  }, SECONDS * 1000);
}
