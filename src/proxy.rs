use std::fmt::Write as _;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, client_async_with_config,
    tungstenite::{client::IntoClientRequest, handshake::client::Response},
};
use url::Url;

const MAX_PROXY_RESPONSE_BYTES: usize = 16 * 1024;

struct WsTarget {
    host: String,
    port: u16,
    is_tls: bool,
}

struct ProxyTarget {
    host: String,
    port: u16,
    auth_header: Option<String>,
}

impl WsTarget {
    fn parse(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).with_context(|| format!("invalid WebSocket URL {url}"))?;
        let is_tls = match parsed.scheme() {
            "ws" => false,
            "wss" => true,
            other => bail!("expected ws:// or wss:// scheme, was {other}"),
        };

        let raw_host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("WebSocket URL missing hostname"))?;
        let host = unbracket_ipv6(raw_host);

        let port = parsed.port().unwrap_or(if is_tls { 443 } else { 80 });

        Ok(Self { host, port, is_tls })
    }
}

impl ProxyTarget {
    fn parse(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).with_context(|| format!("invalid proxy URL {url}"))?;
        if parsed.scheme() != "http" {
            bail!("proxy URL must use http:// scheme");
        }

        let raw_host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("proxy URL missing hostname"))?;
        let host = unbracket_ipv6(raw_host);
        let port = parsed.port().unwrap_or(80);

        let auth_header = if parsed.username().is_empty() {
            None
        } else {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            let username = parsed.username();
            let password = parsed.password().unwrap_or("");
            Some(format!(
                "Basic {}",
                STANDARD.encode(format!("{username}:{password}"))
            ))
        };

        Ok(Self {
            host,
            port,
            auth_header,
        })
    }
}

pub async fn connect_via_proxy<R>(
    request: R,
    proxy_url: &str,
    ignore_ssl_certificate_errors: bool,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response)>
where
    R: IntoClientRequest + Unpin,
{
    let request = request.into_client_request()?;
    let target = WsTarget::parse(request.uri().to_string().as_str())?;
    let proxy = ProxyTarget::parse(proxy_url)?;

    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .with_context(|| format!("could not connect to proxy {}", proxy_url))?;
    send_connect(&mut stream, &target, &proxy).await?;

    if target.is_tls {
        let connector = ignore_ssl_certificate_errors.then(crate::tls::insecure_connector);
        client_async_tls_with_config(request, stream, None, connector)
            .await
            .map_err(|error| error.into())
    } else {
        client_async_with_config(request, MaybeTlsStream::Plain(stream), None)
            .await
            .map_err(|error| error.into())
    }
}

async fn send_connect(
    stream: &mut TcpStream,
    target: &WsTarget,
    proxy: &ProxyTarget,
) -> Result<()> {
    let host_header = format_host_header(&target.host, target.port);
    let mut request = format!(
        "CONNECT {host_header} HTTP/1.1\r\n\
         Host: {host_header}\r\n\
         Proxy-Connection: Keep-Alive\r\n"
    );

    if let Some(auth) = &proxy.auth_header {
        write!(request, "Proxy-Authorization: {auth}\r\n").expect("writing to String never fails");
    }
    request.push_str("\r\n");

    stream
        .write_all(request.as_bytes())
        .await
        .context("could not send proxy CONNECT request")?;
    stream
        .flush()
        .await
        .context("could not flush proxy CONNECT request")?;

    read_connect_response(stream).await
}

async fn read_connect_response(stream: &mut TcpStream) -> Result<()> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        stream
            .read_exact(&mut byte)
            .await
            .context("proxy closed before completing CONNECT response")?;
        buf.push(byte[0]);
        if buf.len() > MAX_PROXY_RESPONSE_BYTES {
            bail!("proxy CONNECT response exceeded {MAX_PROXY_RESPONSE_BYTES} bytes");
        }
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    let response =
        std::str::from_utf8(&buf).context("proxy CONNECT response is not valid UTF-8")?;
    let status_line = response
        .lines()
        .next()
        .context("proxy CONNECT response was empty")?;
    if !status_line.starts_with("HTTP/") {
        bail!("invalid proxy CONNECT response: {status_line}");
    }

    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .context("proxy CONNECT response missing status code")?;
    if !status_code.starts_with('2') {
        bail!("proxy CONNECT failed: {status_line}");
    }

    Ok(())
}

fn format_host_header(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn unbracket_ipv6(host: &str) -> String {
    if host.starts_with('[') && host.ends_with(']') {
        host[1..host.len() - 1].to_owned()
    } else {
        host.to_owned()
    }
}
