//! The client of the `ratls.GrSecret` gRPC service of the grpc_ratls server.
//!
//! The service has one unary call, `GetSecret`, with these messages:
//!
//! ```text
//! message SecretRequest { string name = 1; }
//! message SecretReply { string secret = 1; }
//! ```
//!
//! That is little enough to do without a protobuf compiler and a gRPC stack:
//! HTTP/2 comes from h2, the rest is done here.

use std::net::{TcpStream as StdTcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;
use bytes::{Buf, Bytes};
use h2::client::SendRequest;
use rustls::pki_types::ServerName;
use rustls::{CertificateError, ClientConfig};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::ratls::Result;

pub const METHOD_PATH: &str = "/ratls.GrSecret/GetSecret";
/// The server name for TLS if the host of the address is not a valid one. It is
/// not used for anything, since the server is authenticated by its quote, not
/// by its name.
const FALLBACK_SERVER_NAME: &str = "ratls";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Quote generation and verification take their time, which may include
/// the access to a collateral service
const CALL_TIMEOUT: Duration = Duration::from_secs(300);
/// The largest message accepted
const MAX_MESSAGE_LEN: usize = 1 << 20;

// The status codes of gRPC
pub const GRPC_OK: u32 = 0;
pub const GRPC_NOT_FOUND: u32 = 5;
pub const GRPC_INVALID_ARGUMENT: u32 = 3;
pub const GRPC_UNIMPLEMENTED: u32 = 12;

/// The secrets of the server are base64 encoded in JSON. Be tolerant with the
/// padding, which is not always written.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

pub fn put_varint(buf: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        buf.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn get_varint(buf: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf.first().ok_or("truncated varint")?;
        buf.advance(1);
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint is too long".into())
}

/// A message with one string, `name` of a SecretRequest and `secret` of a
/// SecretReply, both field 1
pub fn encode_string_message(value: &str) -> Vec<u8> {
    let mut message = vec![0x0a]; // field 1, length delimited
    put_varint(&mut message, value.len());
    message.extend_from_slice(value.as_bytes());
    message
}

/// A message as it is sent in HTTP/2 data
pub fn frame(message: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0); // not compressed
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    framed
}

/// A SecretRequest, framed as a gRPC message
fn encode_request(name: &str) -> Vec<u8> {
    frame(&encode_string_message(name))
}

/// The message of a gRPC body with exactly one
pub fn decode_frame(body: &[u8]) -> Result<&[u8]> {
    if body.len() < 5 {
        return Err("the response has no message".into());
    }
    if body[0] != 0 {
        return Err("the response is compressed".into());
    }
    let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    if body.len() - 5 != len {
        return Err("the response has not exactly one message".into());
    }
    Ok(&body[5..])
}

/// The string of a message, as `encode_string_message` makes it (an empty one
/// if it has none), whatever else it holds
pub fn decode_string_message(mut message: &[u8]) -> Result<String> {
    let mut secret = String::new();
    while !message.is_empty() {
        let tag = get_varint(&mut message)?;
        let (field, wire_type) = (tag >> 3, tag & 7);
        match wire_type {
            0 => {
                get_varint(&mut message)?;
            }
            1 | 5 => {
                let len = if wire_type == 1 { 8 } else { 4 };
                if message.len() < len {
                    return Err("truncated message".into());
                }
                message.advance(len);
            }
            2 => {
                let len = get_varint(&mut message)? as usize;
                if message.len() < len {
                    return Err("truncated message".into());
                }
                if field == 1 {
                    secret = String::from_utf8(message[..len].to_vec())?;
                }
                message.advance(len);
            }
            _ => return Err("unsupported wire type in the response".into()),
        }
    }
    Ok(secret)
}

/// What the server sends for a secret is the JSON value of the secret in its
/// configuration, a string with the base64 encoding of the real secret. The
/// real secret is returned.
pub fn decode_secret(secret: &str) -> Result<Vec<u8>> {
    let value: serde_json::Value =
        serde_json::from_str(secret).map_err(|e| format!("the secret is not JSON: {}", e))?;
    decode_base64(value.as_str().ok_or("the secret is not a string")?)
}

/// The bytes of a base64 text, which may have line breaks and may lack the
/// padding
pub fn decode_base64(encoded: &str) -> Result<Vec<u8>> {
    // Line breaks of encoders are no part of the secret
    let encoded: String = encoded
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    BASE64
        .decode(encoded)
        .map_err(|e| format!("the secret is not valid base64: {}", e).into())
}

/// The name of the server in the handshake: the host of the address, as the
/// server may use it to choose its certificate (and complains in its log
/// about a name which it does not know)
fn server_name(addr: &str) -> ServerName<'static> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    ServerName::try_from(host.to_string())
        .unwrap_or_else(|_| ServerName::try_from(FALLBACK_SERVER_NAME).unwrap())
}

/// The reason of a failed handshake. If the certificate of the server was not
/// accepted, it is the reason of the verification and not the way rustls
/// passes it on.
fn handshake_error(e: &std::io::Error) -> String {
    let rustls_error = e.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>());
    match rustls_error {
        Some(rustls::Error::InvalidCertificate(CertificateError::Other(reason))) => {
            reason.0.to_string()
        }
        _ => e.to_string(),
    }
}

pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if (0x20..0x7f).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = |c: u8| (c as char).to_digit(16);
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn grpc_error(headers: &http::HeaderMap) -> Option<String> {
    let status: u32 = headers.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    if status == GRPC_OK {
        return None;
    }
    let message = headers
        .get("grpc-message")
        .and_then(|m| m.to_str().ok())
        .map(percent_decode)
        .unwrap_or_default();
    Some(format!("grpc status {}: {}", status, message))
}

fn check_grpc_status(headers: &http::HeaderMap) -> Result<()> {
    match grpc_error(headers) {
        Some(e) => Err(e.into()),
        None => Ok(()),
    }
}

/// A connection to the server. All the secrets are requested on it.
pub struct SecretClient {
    sender: SendRequest<Bytes>,
    authority: String,
}

impl SecretClient {
    /// Connect to `addr`, which is "host:port", and do the TLS handshake with
    /// the given configuration
    pub async fn connect(addr: &str, tls: Arc<ClientConfig>) -> Result<Self> {
        // The resolution and the connection are blocking, which spares the
        // threads of a blocking pool
        let sock_addrs = addr
            .to_socket_addrs()
            .map_err(|e| format!("cannot resolve {}: {}", addr, e))?;
        // A name may resolve to addresses the server is not reachable on, such
        // as ::1 for localhost, so try them all, in the order of the resolver
        let mut last_error = None;
        let mut connected = None;
        for sock_addr in sock_addrs {
            match StdTcpStream::connect_timeout(&sock_addr, CONNECT_TIMEOUT) {
                Ok(tcp) => {
                    connected = Some(tcp);
                    break;
                }
                Err(e) => last_error = Some(e),
            }
        }
        let tcp = connected.ok_or_else(|| match last_error {
            Some(e) => format!("cannot connect to {}: {}", addr, e),
            None => format!("no address for {}", addr),
        })?;
        tcp.set_nonblocking(true)?;
        tcp.set_nodelay(true)?;
        let tcp = TcpStream::from_std(tcp)?;

        let tls_stream = TlsConnector::from(tls)
            .connect(server_name(addr), tcp)
            .await
            .map_err(|e| {
                format!(
                    "TLS handshake with {} failed: {}",
                    addr,
                    handshake_error(&e)
                )
            })?;

        let (sender, connection) = h2::client::handshake(tls_stream).await?;
        tokio::spawn(async move {
            // The connection ends with the sender, or on an error which the
            // calls report
            let _ = connection.await;
        });
        Ok(Self {
            sender,
            authority: addr.to_string(),
        })
    }

    /// Request the secret `name`. At most `max_len` bytes are accepted.
    pub async fn get_secret(&mut self, name: &str, max_len: usize) -> Result<Vec<u8>> {
        match tokio::time::timeout(CALL_TIMEOUT, self.call(name)).await {
            Ok(reply) => {
                let secret = reply.map_err(|e| format!("cannot get the secret {}: {}", name, e))?;
                if secret.is_empty() {
                    return Err(format!("the server has no secret {}", name).into());
                }
                let decoded = decode_secret(&secret)?;
                if decoded.len() > max_len {
                    return Err(format!(
                        "the secret {} is of {} bytes, more than the {} accepted",
                        name,
                        decoded.len(),
                        max_len
                    )
                    .into());
                }
                Ok(decoded)
            }
            Err(_) => Err(format!("timeout when getting the secret {}", name).into()),
        }
    }

    async fn call(&mut self, name: &str) -> Result<String> {
        let request = http::Request::builder()
            .method("POST")
            .uri(format!("https://{}{}", self.authority, METHOD_PATH))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())?;

        let mut sender = self.sender.clone().ready().await?;
        let (response, mut stream) = sender.send_request(request, false)?;
        stream.send_data(Bytes::from(encode_request(name)), true)?;

        let response = response.await?;
        if response.status() != http::StatusCode::OK {
            return Err(format!("HTTP status {}", response.status()).into());
        }
        // An error may come as the only headers of the response
        check_grpc_status(response.headers())?;

        let mut body = response.into_body();
        let mut data = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            body.flow_control().release_capacity(chunk.len())?;
            if data.len() + chunk.len() > MAX_MESSAGE_LEN {
                return Err("the response is too large".into());
            }
            data.extend_from_slice(&chunk);
        }
        if let Some(trailers) = body.trailers().await? {
            check_grpc_status(&trailers)?;
        } else {
            return Err("the response has no grpc status".into());
        }

        decode_string_message(decode_frame(&data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_framed_protobuf() {
        assert_eq!(
            encode_request("image_key"),
            [&[0, 0, 0, 0, 11, 0x0a, 9][..], b"image_key"].concat()
        );
        // A name needing a two byte length
        let name = "n".repeat(200);
        let framed = encode_request(&name);
        assert_eq!(&framed[..5], &[0, 0, 0, 0, 203]);
        assert_eq!(&framed[5..8], &[0x0a, 0xc8, 0x01]);
        assert_eq!(&framed[8..], name.as_bytes());
    }

    #[test]
    fn reply_is_decoded() {
        let mut message = vec![0x0a, 4];
        message.extend_from_slice(b"\"QQ\"");
        assert_eq!(decode_string_message(&message).unwrap(), "\"QQ\"");
        assert_eq!(decode_string_message(&[]).unwrap(), "");
        // Unknown fields are skipped, also before the secret
        let mut message = vec![0x10, 0x96, 0x01, 0x1a, 2, 1, 2, 0x25, 1, 2, 3, 4];
        message.extend_from_slice(&[0x0a, 1, b'x']);
        assert_eq!(decode_string_message(&message).unwrap(), "x");
        assert!(decode_string_message(&[0x0a, 5, b'x']).is_err());
        assert!(decode_string_message(&[0x0a]).is_err());
        assert!(decode_string_message(&[0x0b]).is_err());
    }

    #[test]
    fn frame_is_checked() {
        assert_eq!(decode_frame(&[0, 0, 0, 0, 2, 7, 8]).unwrap(), &[7, 8]);
        assert!(decode_frame(&[0, 0, 0, 0, 2, 7]).is_err());
        assert!(decode_frame(&[0, 0, 0, 0, 2, 7, 8, 9]).is_err());
        assert!(decode_frame(&[1, 0, 0, 0, 0]).is_err());
        assert!(decode_frame(&[0, 0, 0]).is_err());
    }

    #[test]
    fn secret_is_base64_in_json() {
        assert_eq!(decode_secret("\"YWJj\"").unwrap(), b"abc");
        // With and without padding, as encoders do
        assert_eq!(decode_secret("\"YWI=\"").unwrap(), b"ab");
        assert_eq!(decode_secret("\"YWI\"").unwrap(), b"ab");
        // The length is exact, whatever it is
        assert_eq!(decode_secret("\"YQ==\"").unwrap(), b"a");
        // Wrapped lines
        assert_eq!(decode_secret("\"YW\\nJj\\n\"").unwrap(), b"abc");
        assert!(decode_secret("\"Y!Jj\"").is_err());
        assert!(decode_secret("YWJj").is_err());
        assert!(decode_secret("42").is_err());
    }

    #[test]
    fn server_name_is_the_host() {
        let name = |addr| server_name(addr).to_str().into_owned();
        assert_eq!(name("localhost:50051"), "localhost");
        assert_eq!(name("kms.example.org:1"), "kms.example.org");
        assert_eq!(name("192.168.70.140:50051"), "192.168.70.140");
        assert_eq!(name("[::1]:50051"), "::1");
        assert_eq!(name("not a name:50051"), "ratls");
    }

    #[test]
    fn grpc_message_is_percent_encoded() {
        assert_eq!(percent_encode("no secret\n100%"), "no secret%0A100%25");
        assert_eq!(percent_decode(&percent_encode("ä é\n%x")), "ä é\n%x");
    }

    #[test]
    fn grpc_message_is_percent_decoded() {
        assert_eq!(percent_decode("no%20secret%2"), "no secret%2");
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());
        assert!(grpc_error(&headers).is_none());
        headers.insert("grpc-status", "1".parse().unwrap());
        headers.insert("grpc-message", "gone%21".parse().unwrap());
        assert_eq!(grpc_error(&headers).unwrap(), "grpc status 1: gone!");
    }
}
