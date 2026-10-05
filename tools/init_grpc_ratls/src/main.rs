extern crate libc;
extern crate serde;
extern crate serde_json;

use libc::syscall;
use serde::Deserialize;

use std::env;
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::str;
use std::sync::Arc;

use occlum_ratls::grpc;
use occlum_ratls::ratls::{self, Dcap, Policy, RAConfig, Result};

/// The longest secret accepted
const MAX_SECRET_LEN: usize = 10240;

#[derive(Deserialize, Debug, Clone)]
struct KmsKeys {
    key: String,
    path: String,
}

#[derive(Deserialize, Debug)]
struct InitRAConfig {
    kms_server: String,
    kms_keys: Vec<KmsKeys>,
    ra_config: RAConfig,
}

fn load_ra_config(ra_conf_path: &str) -> Result<InitRAConfig> {
    let mut ra_conf_file = File::open(ra_conf_path)?;
    let ra_conf = {
        let mut ra_conf = String::new();
        ra_conf_file.read_to_string(&mut ra_conf)?;
        ra_conf
    };
    let config: InitRAConfig = serde_json::from_str(&ra_conf)?;
    Ok(config)
}

/// Request the secrets from the server at `server_addr`, which has to be
/// attested as the configuration says. One connection and one quote of ours
/// are enough for all of them.
fn get_secrets(server_addr: &str, ra_config: &RAConfig, names: &[&str]) -> Result<Vec<Vec<u8>>> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let tls = ratls::client_config(Arc::new(Dcap), Policy::from_config(ra_config)?)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut client = grpc::SecretClient::connect(server_addr, tls).await?;
        let mut secrets = Vec::with_capacity(names.len());
        for name in names {
            // Keep the secrets in buffers, not in the file system, for better security
            secrets.push(client.get_secret(name, MAX_SECRET_LEN).await?);
        }
        Ok(secrets)
    })
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    // Load the configuration from initfs
    const IMAGE_CONFIG_FILE: &str = "/etc/image_config.json";
    const INIT_RA_CONF: &str = "/etc/init_ra_conf.json";
    let image_config = load_config(IMAGE_CONFIG_FILE)?;

    // Do parse to get Init RA information
    let init_ra_conf = load_ra_config(INIT_RA_CONF)?;

    // grpc server address from environment has higher priority
    let server_addr = env::var("OCCLUM_INIT_RA_KMS_SERVER").unwrap_or(init_ra_conf.kms_server);

    // The key of the FS image is the first secret, if needed
    let encrypted = match &image_config.image_type[..] {
        "encrypted" => true,
        "integrity-only" => false,
        _ => unreachable!(),
    };
    let mut names: Vec<&str> = Vec::new();
    if encrypted {
        names.push("image_key");
    }
    names.extend(init_ra_conf.kms_keys.iter().map(|keys| keys.key.as_str()));

    let mut secrets = get_secrets(&server_addr, &init_ra_conf.ra_config, &names)?.into_iter();

    let key = if encrypted {
        let key_string = String::from_utf8(secrets.next().unwrap())
            .map_err(|_| "the image key is not a string")?;
        let key_str = key_string
            .trim_end_matches(|c| c == '\r' || c == '\n')
            .to_string();
        let mut key: sgx_key_128bit_t = Default::default();
        parse_str_to_bytes(&key_str, &mut key)?;
        Some(key)
    } else {
        None
    };
    let key_ptr = key
        .as_ref()
        .map(|key| key as *const sgx_key_128bit_t)
        .unwrap_or(std::ptr::null());

    // Mount the image
    const SYS_MOUNT_FS: i64 = 363;
    // User can provide valid path for runtime mount and boot
    // Otherwise, just pass null pointer to do general mount and boot
    let root_config_path: *const i8 = std::ptr::null();
    let ret = unsafe { syscall(SYS_MOUNT_FS, key_ptr, root_config_path) };
    if ret < 0 {
        return Err(Box::new(std::io::Error::last_os_error()));
    }

    // Save the keys to their paths
    for (keys, secret) in init_ra_conf.kms_keys.iter().zip(secrets) {
        fs::write(&keys.path, secret)?;
    }

    Ok(())
}

#[allow(non_camel_case_types)]
type sgx_key_128bit_t = [u8; 16];

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct ImageConfig {
    image_type: String,
}

fn load_config(config_path: &str) -> Result<ImageConfig> {
    let mut config_file = File::open(config_path)?;
    let config_json = {
        let mut config_json = String::new();
        config_file.read_to_string(&mut config_json)?;
        config_json
    };
    let config: ImageConfig = serde_json::from_str(&config_json)?;
    Ok(config)
}

fn parse_str_to_bytes(arg_str: &str, bytes: &mut [u8]) -> Result<()> {
    let bytes_str_vec = {
        let bytes_str_vec: Vec<&str> = arg_str.split('-').collect();
        if bytes_str_vec.len() != bytes.len() {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                "The length or format of Key/MAC string is invalid",
            )));
        }
        bytes_str_vec
    };

    for (byte_i, byte_str) in bytes_str_vec.iter().enumerate() {
        bytes[byte_i] = u8::from_str_radix(byte_str, 16)?;
    }
    Ok(())
}
