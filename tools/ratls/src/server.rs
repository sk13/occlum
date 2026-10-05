//! The server of the `ratls.GrSecret` gRPC service (see `grpc`), over RA-TLS.
//!
//! One server holds the secrets of several clients, and answers a request
//! with the secret of the client the peer is: the peer is attested, and its
//! measurements, as its quote gives them, must be allowed by the policy of
//! exactly one client. Whatever the peer asks for, it gets only the secrets
//! of that client, so the clients need no different names for their secrets
//! (they all ask for "image_key", for example).
//!
//! The protocol is the one of the server of the grpc_ratls toolchain, with
//! `init_grpc_ratls` as the client (the replies are the same, but this is not
//! tested with the C client library).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use h2::server::SendResponse;
use h2::RecvStream;
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;

use crate::grpc::{self, GRPC_INVALID_ARGUMENT, GRPC_NOT_FOUND, GRPC_OK, GRPC_UNIMPLEMENTED};
use crate::ratls::{self, Attestation, Measurement, Policy, Result};

/// The time a peer has for the handshake, the verification of its quote and
/// the setup of HTTP/2
const SETUP_TIMEOUT: Duration = Duration::from_secs(120);
/// The largest request accepted, which has no more than the name of a secret
const MAX_REQUEST_LEN: usize = 64 * 1024;
/// The number of connections served at a time
const MAX_CONNECTIONS: usize = 256;

type Connection = h2::server::Connection<tokio_rustls::server::TlsStream<TcpStream>, Bytes>;

/// A client of the server: the enclaves it is, and their secrets
pub struct Client {
    name: String,
    policy: Policy,
    /// The reply for each secret, which is its base64 encoding as a JSON
    /// string, as the server of the toolchain sends it
    replies: HashMap<String, String>,
}

impl Client {
    /// `name` is for the log. `secrets` are the secrets by name, as they are.
    pub fn new(name: &str, policy: Policy, secrets: HashMap<String, Vec<u8>>) -> Result<Self> {
        // A policy which does not tell the enclaves apart is no policy for
        // somebody whose secrets are for one enclave
        if !policy.verifies_identity() {
            return Err(format!(
                "the policy of {} neither verifies mr_enclave nor mr_signer",
                name
            )
            .into());
        }
        let replies = secrets
            .into_iter()
            .map(|(secret, value)| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(value);
                (secret, serde_json::Value::String(encoded).to_string())
            })
            .collect();
        Ok(Self {
            name: name.to_string(),
            policy,
            replies,
        })
    }
}

pub struct Server {
    attestation: Arc<dyn Attestation>,
    clients: Vec<Client>,
    tls: Arc<ServerConfig>,
}

impl Server {
    /// A server with the secrets of the clients. Its quote is generated here.
    pub fn new(attestation: Arc<dyn Attestation>, clients: Vec<Client>) -> Result<Arc<Self>> {
        if clients.is_empty() {
            return Err("the server has no clients".into());
        }
        for (i, client) in clients.iter().enumerate() {
            if clients[..i].iter().any(|c| c.name == client.name) {
                return Err(format!("the client {} is twice", client.name).into());
            }
        }
        let tls = ratls::server_config(attestation.as_ref())?;
        Ok(Arc::new(Self {
            attestation,
            clients,
            tls,
        }))
    }

    /// The client that has the measurements, which must be one only
    fn match_client(&self, measurement: &Measurement) -> Result<usize> {
        let mut matching = self
            .clients
            .iter()
            .enumerate()
            .filter(|(_, client)| client.policy.allows(measurement))
            .map(|(i, _)| i);
        match (matching.next(), matching.next()) {
            (Some(i), None) => Ok(i),
            (None, _) => Err("the SGX measurements of the peer are those of no client".into()),
            (Some(_), Some(_)) => {
                Err("the SGX measurements of the peer are those of several clients".into())
            }
        }
    }

    /// Serve the connections to the listener, for ever
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        loop {
            let (tcp, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    // Out of file descriptors, for example, which may pass
                    eprintln!("ratls_kms: cannot accept a connection: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            match connections.clone().try_acquire_owned() {
                Ok(permit) => {
                    tokio::spawn(self.clone().connection(tcp, permit));
                }
                Err(_) => eprintln!("ratls_kms: too many connections, one is refused"),
            }
        }
    }

    async fn connection(self: Arc<Self>, tcp: TcpStream, _permit: OwnedSemaphorePermit) {
        let _ = tcp.set_nodelay(true);
        let (connection, client) = match tokio::time::timeout(SETUP_TIMEOUT, self.setup(tcp)).await
        {
            Ok(Ok(accepted)) => accepted,
            Ok(Err(e)) => {
                eprintln!("ratls_kms: a peer is refused: {}", e);
                return;
            }
            Err(_) => {
                eprintln!("ratls_kms: a peer is refused: no handshake in time");
                return;
            }
        };
        self.requests(connection, client).await;
    }

    /// The handshake, and the attestation of the peer, which gives the client
    async fn setup(&self, tcp: TcpStream) -> Result<(Connection, usize)> {
        let stream = TlsAcceptor::from(self.tls.clone()).accept(tcp).await?;
        let cert = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or("the peer has no certificate")?
            .to_vec();

        // Nothing is served before the peer is attested. The verification of
        // a quote takes its time, not on the threads of the connections.
        let attestation = self.attestation.clone();
        let (measurement, _) = tokio::task::spawn_blocking(move || {
            ratls::verify_peer_quote(&cert, attestation.as_ref())
        })
        .await??;
        let client = self
            .match_client(&measurement)
            .map_err(|e| format!("{}\n{}", e, measurement))?;
        println!(
            "ratls_kms: {} is connected, MRENCLAVE {}",
            self.clients[client].name,
            measurement
                .mr_enclave
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        );

        let connection = h2::server::handshake(stream).await?;
        Ok((connection, client))
    }

    async fn requests(self: &Arc<Self>, mut connection: Connection, client: usize) {
        while let Some(accepted) = connection.accept().await {
            match accepted {
                Ok((request, respond)) => {
                    let server = self.clone();
                    tokio::spawn(async move { server.request(client, request, respond).await });
                }
                Err(_) => break,
            }
        }
    }

    async fn request(
        &self,
        client: usize,
        request: http::Request<RecvStream>,
        mut respond: SendResponse<Bytes>,
    ) {
        let client = &self.clients[client];
        let answer = self.answer(client, request).await;

        let sent = match &answer {
            Ok(secret) => send_secret(&mut respond, secret),
            Err((status, message)) => send_error(&mut respond, *status, message),
        };
        // The peer may be gone, which is its own affair
        let _ = sent;
    }

    /// The reply for a request of the client: the JSON of the secret, or the
    /// status of gRPC and a message
    async fn answer(
        &self,
        client: &Client,
        request: http::Request<RecvStream>,
    ) -> std::result::Result<String, (u32, String)> {
        if request.method() != http::Method::POST || request.uri().path() != grpc::METHOD_PATH {
            return Err((GRPC_UNIMPLEMENTED, "no such method".into()));
        }

        let mut body = request.into_body();
        let mut data = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.map_err(|e| (GRPC_INVALID_ARGUMENT, e.to_string()))?;
            let _ = body.flow_control().release_capacity(chunk.len());
            if data.len() + chunk.len() > MAX_REQUEST_LEN {
                return Err((GRPC_INVALID_ARGUMENT, "the request is too large".into()));
            }
            data.extend_from_slice(&chunk);
        }
        let name = grpc::decode_frame(&data)
            .and_then(grpc::decode_string_message)
            .map_err(|e| (GRPC_INVALID_ARGUMENT, e.to_string()))?;

        // The names of the secrets come from the peer, so they are shown escaped
        let shown: String = format!("{:?}", name).chars().take(100).collect();
        match client.replies.get(&name) {
            Some(reply) => {
                println!("ratls_kms: {} gets {}", client.name, shown);
                Ok(reply.clone())
            }
            None => {
                eprintln!(
                    "ratls_kms: {} asks for {}, which it has not",
                    client.name, shown
                );
                Err((GRPC_NOT_FOUND, "no such secret".into()))
            }
        }
    }
}

fn send_secret(
    respond: &mut SendResponse<Bytes>,
    secret: &str,
) -> std::result::Result<(), h2::Error> {
    let response = http::Response::builder()
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();
    let mut stream = respond.send_response(response, false)?;
    let reply = grpc::frame(&grpc::encode_string_message(secret));
    stream.send_data(Bytes::from(reply), false)?;
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", GRPC_OK.to_string().parse().unwrap());
    stream.send_trailers(trailers)
}

/// An error is sent as a response which is only trailers
fn send_error(
    respond: &mut SendResponse<Bytes>,
    status: u32,
    message: &str,
) -> std::result::Result<(), h2::Error> {
    let response = http::Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-status", status.to_string())
        .header("grpc-message", grpc::percent_encode(message))
        .body(())
        .unwrap();
    respond.send_response(response, true).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::SecretClient;
    use crate::ratls::tests::{config, FakeAttestation};
    use crate::ratls::{client_config, Policy};

    fn hex(bytes: &[u8; 32]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn enclave(id: u8) -> FakeAttestation {
        let mut att = FakeAttestation::new();
        att.measurement.mr_enclave = [id; 32];
        att
    }

    fn client(name: &str, id: u8, secrets: &[(&str, &str)]) -> Client {
        let policy = Policy::from_config(&config(&hex(&[id; 32]))).unwrap();
        let secrets = secrets
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect();
        Client::new(name, policy, secrets).unwrap()
    }

    /// Start a server with the clients, in this thread's runtime; its address
    fn start(clients: Vec<Client>) -> (String, tokio::task::JoinHandle<Result<()>>) {
        let server = Server::new(Arc::new(enclave(0x50)), clients).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let listener = TcpListener::from_std(listener).unwrap();
        (addr, tokio::spawn(server.serve(listener)))
    }

    /// Get the secrets with the enclave `id`, which trusts the server `0x50`
    async fn get(addr: &str, id: u8, names: &[&str]) -> Result<Vec<Vec<u8>>> {
        let tls = client_config(
            Arc::new(enclave(id)),
            Policy::from_config(&config(&hex(&[0x50; 32]))).unwrap(),
        )?;
        let mut client = SecretClient::connect(addr, tls).await?;
        let mut secrets = Vec::new();
        for name in names {
            secrets.push(client.get_secret(name, 100).await?);
        }
        Ok(secrets)
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn each_client_gets_its_own_secrets_by_the_same_name() {
        runtime().block_on(async {
            let (addr, _) = start(vec![
                client("a", 0x11, &[("image_key", "key of a"), ("cert", "pem a\n")]),
                client("b", 0x12, &[("image_key", "key of b")]),
            ]);
            assert_eq!(
                get(&addr, 0x11, &["image_key", "cert", "image_key"])
                    .await
                    .unwrap(),
                [&b"key of a"[..], &b"pem a\n"[..], &b"key of a"[..]]
            );
            assert_eq!(
                get(&addr, 0x12, &["image_key"]).await.unwrap(),
                [&b"key of b"[..]]
            );
        });
    }

    #[test]
    fn a_client_has_no_secrets_of_another() {
        runtime().block_on(async {
            let (addr, _) = start(vec![
                client("a", 0x11, &[("image_key", "key of a")]),
                client("b", 0x12, &[("image_key", "key of b"), ("only_b", "x")]),
            ]);
            let err = get(&addr, 0x11, &["only_b"]).await.unwrap_err().to_string();
            assert!(
                err.contains("only_b") && err.contains("no such secret"),
                "{}",
                err
            );
            // The server goes on, for this client as for others
            assert_eq!(
                get(&addr, 0x11, &["image_key"]).await.unwrap(),
                [&b"key of a"[..]]
            );
        });
    }

    #[test]
    fn an_unknown_enclave_gets_nothing() {
        runtime().block_on(async {
            let (addr, _) = start(vec![client("a", 0x11, &[("image_key", "key of a")])]);
            let err = get(&addr, 0x13, &["image_key"])
                .await
                .unwrap_err()
                .to_string();
            assert!(!err.contains("key of a"), "{}", err);
            assert_eq!(
                get(&addr, 0x11, &["image_key"]).await.unwrap(),
                [&b"key of a"[..]]
            );
        });
    }

    #[test]
    fn an_enclave_of_several_clients_gets_nothing() {
        runtime().block_on(async {
            let (addr, _) = start(vec![
                client("a", 0x11, &[("image_key", "key of a")]),
                client("same as a", 0x11, &[("image_key", "key of the same")]),
                client("b", 0x12, &[("image_key", "key of b")]),
            ]);
            assert!(get(&addr, 0x11, &["image_key"]).await.is_err());
            assert_eq!(
                get(&addr, 0x12, &["image_key"]).await.unwrap(),
                [&b"key of b"[..]]
            );
        });
    }

    #[test]
    fn a_quote_which_is_not_valid_gets_nothing() {
        runtime().block_on(async {
            let mut server_att = enclave(0x50);
            server_att.verify_result = Err("revoked".into());
            let server = Server::new(
                Arc::new(server_att),
                vec![client("a", 0x11, &[("image_key", "key of a")])],
            )
            .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            tokio::spawn(server.serve(TcpListener::from_std(listener).unwrap()));
            assert!(get(&addr, 0x11, &["image_key"]).await.is_err());
        });
    }

    #[test]
    fn the_server_is_attested_by_the_client() {
        runtime().block_on(async {
            let (addr, _) = start(vec![client("a", 0x11, &[("image_key", "key of a")])]);
            let tls = client_config(
                Arc::new(enclave(0x11)),
                Policy::from_config(&config(&hex(&[0x77; 32]))).unwrap(),
            )
            .unwrap();
            let err = SecretClient::connect(&addr, tls)
                .await
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains("allowable list"), "{}", err);
        });
    }

    #[test]
    fn clients_need_a_policy_on_the_identity() {
        let mut policy = config(&hex(&[1; 32]));
        policy.verify_mr_enclave = "off".into();
        let policy = Policy::from_config(&policy).unwrap();
        assert!(Client::new("a", policy, HashMap::new()).is_err());
        assert!(Server::new(Arc::new(enclave(0x50)), Vec::new()).is_err());
        let twice = vec![client("a", 1, &[]), client("a", 2, &[])];
        assert!(Server::new(Arc::new(enclave(0x50)), twice).is_err());
    }
}
