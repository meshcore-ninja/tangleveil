// extract-analyzers.ts

const DATA_URL = "https://meshcore.ninja/data.json";

interface Analyzer {
  name?: string;
  url: string;
  headers?: Record<string, string>;
}

interface Network {
  id: string;
  analyzers?: Analyzer[];
}

interface Data {
  networks?: Network[];
}

function websocketUrl(rawUrl: string): string {
  const url = new URL(rawUrl);

  if (url.protocol === "https:") {
    url.protocol = "wss:";
  } else if (url.protocol === "http:") {
    url.protocol = "ws:";
  } else if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error(`Unsupported protocol: ${url.protocol}`);
  }

  const path = url.pathname.replace(/\/+$/, "");

  if (!path.endsWith("/ws")) {
    url.pathname = `${path}/ws`;
  }

  url.hash = "";

  return url.toString();
}

function tomlString(value: string): string {
  return JSON.stringify(value);
}

function tomlHeaders(headers: Record<string, string>): string {
  const values = Object.entries(headers)
    .map(([key, value]) => `${key} = ${tomlString(value)}`)
    .join(", ");

  return `{ ${values} }`;
}

const response = await fetch(DATA_URL);

if (!response.ok) {
  throw new Error(
    `Failed to fetch ${DATA_URL}: ${response.status} ${response.statusText}`,
  );
}

const data = (await response.json()) as Data;
const blocks: string[] = [];
const seenUrls = new Set<string>();

for (const network of data.networks ?? []) {
  let sourceNumber = 0;

  for (const analyzer of network.analyzers ?? []) {
    try {
      const url = websocketUrl(analyzer.url);

      // Do not emit the same analyzer more than once.
      if (seenUrls.has(url)) {
        continue;
      }

      seenUrls.add(url);
      sourceNumber++;

      const id =
        sourceNumber === 1
          ? network.id
          : `${network.id}-${sourceNumber}`;

      const lines = [
        "[[sources]]",
        `id = ${tomlString(id)}`,
        `url = ${tomlString(url)}`,
        `mapping = ${tomlString(id + ':' + (sourceNumber-1))}`,
      ];

      if (analyzer.headers && Object.keys(analyzer.headers).length > 0) {
        lines.push(`headers = ${tomlHeaders(analyzer.headers)}`);
      }

      blocks.push(lines.join("\n"));
    } catch (error) {
      console.error(
        `Skipping analyzer ${analyzer.url}:`,
        error instanceof Error ? error.message : error,
      );
    }
  }
}

console.log(blocks.join("\n\n"));