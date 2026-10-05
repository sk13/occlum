//! A key server for enclaves with remote attestation, which `init_grpc_ratls`
//! is the client of, with secrets for several enclaves in one server. An enclave that asks for a
//! secret gets the one of that name of the client it is (see
//! `occlum_ratls::server`), so all clients can have an `image_key`.
//!
//!     ratls_kms <address to listen on> <configuration>
//!
//! The configuration is JSON:
//!
//! ```json
//! {
//!     "clients": [
//!         {
//!             "name": "nrf",
//!             "ra": {
//!                 "verify_mr_enclave": "on", "verify_mr_signer": "on",
//!                 "verify_isv_prod_id": "on", "verify_isv_svn": "on",
//!                 "verify_config_svn": "off", "verify_enclave_debuggable": "on",
//!                 "sgx_mrs": [{"mr_enclave": "...", "mr_signer": "...",
//!                              "isv_prod_id": 3, "isv_svn": 1, "config_svn": 0,
//!                              "debuggable": false}]
//!             },
//!             "secrets": {"demo_key": "<base64>"},
//!             "secrets_dir": "/kms/nrf"
//!         }
//!     ],
//!     "worker_threads": 2
//! }
//! ```
//!
//! `ra` is the configuration of the verification of the peers as in the one of
//! `init_grpc_ratls`; a peer is the client
//! if it is allowed by it. A peer which is allowed by the `ra` of several
//! clients, or of none, gets nothing. It has to verify MRENCLAVE or MRSIGNER.
//! The secrets are in `secrets`, by name with the base64 of their value, and
//! in the files of `secrets_dir`, named after the secrets, with their value.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::exit;
use std::sync::Arc;

use occlum_ratls::grpc::decode_base64;
use occlum_ratls::ratls::{Dcap, Policy, RAConfig, Result};
use occlum_ratls::server::{Client, Server};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};

/// The largest secret
const MAX_SECRET_LEN: u64 = 512 * 1024;

const DEFAULT_WORKER_THREADS: usize = 2;
/// The threads for the verification of quotes, which may take long
const BLOCKING_THREADS: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    clients: Vec<ClientConfig>,
    worker_threads: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    name: String,
    ra: RAConfig,
    #[serde(default)]
    secrets: HashMap<String, String>,
    secrets_dir: Option<String>,
}

fn load_secrets(client: &ClientConfig) -> Result<HashMap<String, Vec<u8>>> {
    let mut secrets = HashMap::new();
    for (name, encoded) in &client.secrets {
        let value = decode_base64(encoded).map_err(|e| format!("secret {}: {}", name, e))?;
        secrets.insert(name.clone(), value);
    }
    if let Some(dir) = &client.secrets_dir {
        for entry in fs::read_dir(dir).map_err(|e| format!("cannot read {}: {}", dir, e))? {
            let path = entry?.path();
            let name = path.file_name().and_then(|n| n.to_str());
            let name = match name {
                Some(name) if !name.starts_with('.') && path.is_file() => name,
                _ => continue,
            };
            if path.metadata()?.len() > MAX_SECRET_LEN {
                return Err(format!("{} is too large", path.display()).into());
            }
            if secrets.insert(name.to_string(), fs::read(&path)?).is_some() {
                return Err(format!("secret {} of {} is twice", name, client.name).into());
            }
        }
    }
    for (name, value) in &secrets {
        if name.is_empty() || value.is_empty() || value.len() as u64 > MAX_SECRET_LEN {
            return Err(
                format!("secret {:?} of {} is empty or too large", name, client.name).into(),
            );
        }
    }
    Ok(secrets)
}

fn load_server(config_path: &str) -> Result<(Arc<Server>, usize)> {
    let config: Config = serde_json::from_str(
        &fs::read_to_string(config_path)
            .map_err(|e| format!("cannot read {}: {}", config_path, e))?,
    )
    .map_err(|e| format!("{}: {}", config_path, e))?;

    let mut clients = Vec::new();
    for client in &config.clients {
        let secrets = load_secrets(client)?;
        println!(
            "ratls_kms: client {}: {}",
            client.name,
            if secrets.is_empty() {
                "no secrets".to_string()
            } else {
                let mut names: Vec<_> = secrets.keys().map(|n| n.as_str()).collect();
                names.sort();
                names.join(", ")
            }
        );
        let policy = Policy::from_config(&client.ra)
            .map_err(|e| format!("client {}: {}", client.name, e))?;
        clients.push(Client::new(&client.name, policy, secrets)?);
    }
    Ok((
        Server::new(Arc::new(Dcap), clients)?,
        config.worker_threads.unwrap_or(DEFAULT_WORKER_THREADS),
    ))
}

async fn run(server: Arc<Server>, addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen on {}: {}", addr, e))?;
    println!("ratls_kms: listening on {}", addr);

    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        result = server.serve(listener) => result,
        _ = term.recv() => Ok(()),
        _ = int.recv() => Ok(()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 || !Path::new(&args[2]).exists() {
        eprintln!("usage: {} <address to listen on> <configuration>", args[0]);
        exit(2);
    }
    let result = load_server(&args[2]).and_then(|(server, worker_threads)| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads.max(1))
            .max_blocking_threads(BLOCKING_THREADS)
            .enable_all()
            .build()?
            .block_on(run(server, &args[1]))
    });
    if let Err(e) = result {
        eprintln!("ratls_kms: {}", e);
        exit(1);
    }
}
