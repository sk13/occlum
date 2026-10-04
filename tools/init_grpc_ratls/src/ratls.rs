//! RA-TLS on top of rustls.
//!
//! Both peers authenticate with a self-signed certificate that carries a DCAP
//! quote in a custom extension. The quote binds the certificate by holding
//! the SHA-256 of its SubjectPublicKeyInfo in the report data. Nothing about
//! the usual PKI is checked (chain, names, validity); the trust decision is
//! made on the quote and the measurements allowed by the configuration.
//!
//! This is wire compatible with the grpc_ratls server of the toolchain.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use rcgen::{CertificateParams, CustomExtension, KeyPair, PublicKeyData};
use ring::digest;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::crypto::{ring as ring_provider, verify_tls13_signature_with_raw_key};
use rustls::pki_types::{
    CertificateDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer, UnixTime,
};
use rustls::{CertificateError, ClientConfig, DigitallySignedStruct, OtherError, SignatureScheme};
use serde::Deserialize;
use x509_parser::der_parser::oid::Oid;
use x509_parser::prelude::{FromDer, X509Certificate};

/// OID of the X.509 extension holding the quote
const QUOTE_OID: [u64; 5] = [1, 2, 840, 113741, 1];

/// The ALPN protocol gRPC needs on top of TLS
const ALPN_H2: &[u8] = b"h2";

// Layout of an SGX quote: a 48 bytes header followed by the 384 bytes report
// body (sgx_quote3_t, sgx_report_body_t).
const QUOTE_HEADER_LEN: usize = 48;
const REPORT_BODY_LEN: usize = 384;
const FLAGS_OFFSET: usize = QUOTE_HEADER_LEN + 48;
const MR_ENCLAVE_OFFSET: usize = QUOTE_HEADER_LEN + 64;
const MR_SIGNER_OFFSET: usize = QUOTE_HEADER_LEN + 128;
const ISV_PROD_ID_OFFSET: usize = QUOTE_HEADER_LEN + 256;
const ISV_SVN_OFFSET: usize = QUOTE_HEADER_LEN + 258;
const CONFIG_SVN_OFFSET: usize = QUOTE_HEADER_LEN + 260;
const REPORT_DATA_OFFSET: usize = QUOTE_HEADER_LEN + 320;
const SGX_FLAGS_DEBUG: u64 = 0x2;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct RaError(String);

impl fmt::Display for RaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for RaError {}

macro_rules! ra_err {
    ($($arg:tt)*) => { Box::<dyn Error + Send + Sync>::from(RaError(format!($($arg)*))) };
}

/// The outcome of the quote verification of the DCAP library
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum QuoteStatus {
    /// Everything is fine
    Ok,
    /// The quote is valid but the platform needs some update or configuration
    /// (the non-terminal results of the DCAP quote verification library)
    NeedsAttention(u32),
}

/// What RA-TLS needs from the platform: quotes for our own certificate and the
/// verification of the quote of the peer.
pub trait Attestation: Send + Sync {
    fn generate_quote(&self, report_data: &[u8; 64]) -> Result<Vec<u8>>;
    fn verify_quote(&self, quote: &[u8]) -> Result<QuoteStatus>;
}

/// The attestation of the SGX hardware, through `/dev/sgx` of Occlum
pub struct Dcap;

// Values of sgx_ql_qv_result_t
const QV_RESULT_OK: u32 = 0;
const QV_RESULT_CONFIG_NEEDED: u32 = 0xA001;
const QV_RESULT_OUT_OF_DATE: u32 = 0xA002;
const QV_RESULT_OUT_OF_DATE_CONFIG_NEEDED: u32 = 0xA003;
const QV_RESULT_SW_HARDENING_NEEDED: u32 = 0xA007;
const QV_RESULT_CONFIG_AND_SW_HARDENING_NEEDED: u32 = 0xA008;

impl Attestation for Dcap {
    fn generate_quote(&self, report_data: &[u8; 64]) -> Result<Vec<u8>> {
        let mut dcap = occlum_dcap::DcapQuote::new()?;
        let result = (|| {
            let quote_size = dcap.get_quote_size()?;
            let mut quote = vec![0u8; quote_size as usize];
            let report_data = occlum_dcap::sgx_report_data_t { d: *report_data };
            dcap.generate_quote(quote.as_mut_ptr(), &report_data)?;
            Ok(quote)
        })();
        dcap.close();
        result
    }

    fn verify_quote(&self, quote: &[u8]) -> Result<QuoteStatus> {
        let quote_size = u32::try_from(quote.len())?;
        let mut dcap = occlum_dcap::DcapQuote::new()?;
        let result = (|| {
            let supplemental_size = dcap.get_supplemental_data_size()?;
            let mut supplemental = vec![0u8; supplemental_size as usize];
            let mut collateral_expiration_status: u32 = 1;
            // sgx_ql_qv_result_t is a C enum, which must never hold a value
            // out of its range. Therefore read it as a plain integer.
            let mut qv_result: u32 = 0xA006; // SGX_QL_QV_RESULT_UNSPECIFIED
            let mut arg = occlum_dcap::IoctlVerDCAPQuoteArg {
                quote_buf: quote.as_ptr(),
                quote_size,
                collateral_expiration_status: &mut collateral_expiration_status,
                quote_verification_result: &mut qv_result as *mut u32 as *mut _,
                supplemental_data_size: supplemental_size,
                supplemental_data: supplemental.as_mut_ptr(),
            };
            dcap.verify_quote(&mut arg)?;

            if collateral_expiration_status != 0 {
                eprintln!("RA-TLS: the verification collateral has expired");
            }
            match qv_result {
                QV_RESULT_OK => Ok(QuoteStatus::Ok),
                QV_RESULT_CONFIG_NEEDED
                | QV_RESULT_OUT_OF_DATE
                | QV_RESULT_OUT_OF_DATE_CONFIG_NEEDED
                | QV_RESULT_SW_HARDENING_NEEDED
                | QV_RESULT_CONFIG_AND_SW_HARDENING_NEEDED => {
                    eprintln!(
                        "RA-TLS: quote verified with the non-terminal result {:#x}",
                        qv_result
                    );
                    Ok(QuoteStatus::NeedsAttention(qv_result))
                }
                // INVALID_SIGNATURE, REVOKED, UNSPECIFIED and anything else
                _ => Err(ra_err!(
                    "the quote verification ended with the terminal result {:#x}",
                    qv_result
                )),
            }
        })();
        dcap.close();
        result
    }
}

/// The identity of an enclave as given by its quote
#[derive(Debug, PartialEq, Eq)]
pub struct Measurement {
    pub mr_enclave: [u8; 32],
    pub mr_signer: [u8; 32],
    pub isv_prod_id: u16,
    pub isv_svn: u16,
    pub config_svn: u16,
    pub debuggable: bool,
    pub report_data: [u8; 64],
}

impl Measurement {
    pub fn from_quote(quote: &[u8]) -> Result<Self> {
        if quote.len() < QUOTE_HEADER_LEN + REPORT_BODY_LEN {
            return Err(ra_err!("the quote is too short ({} bytes)", quote.len()));
        }
        // The version is 3 for ECDSA quotes of the first DCAP generation and 4
        // for the second. The report body of an SGX enclave is the same, the
        // TEE type follows the version in the latter (0 is SGX). In the former,
        // it is a reserved field which is 0.
        let version = u16::from_le_bytes([quote[0], quote[1]]);
        let tee_type = u32::from_le_bytes(quote[4..8].try_into().unwrap());
        if !(version == 3 || version == 4) || tee_type != 0 {
            return Err(ra_err!(
                "the quote is not the one of an SGX enclave (version {}, TEE type {})",
                version,
                tee_type
            ));
        }

        let u16_at = |off: usize| u16::from_le_bytes([quote[off], quote[off + 1]]);
        let flags = u64::from_le_bytes(quote[FLAGS_OFFSET..FLAGS_OFFSET + 8].try_into().unwrap());
        Ok(Self {
            mr_enclave: quote[MR_ENCLAVE_OFFSET..MR_ENCLAVE_OFFSET + 32]
                .try_into()
                .unwrap(),
            mr_signer: quote[MR_SIGNER_OFFSET..MR_SIGNER_OFFSET + 32]
                .try_into()
                .unwrap(),
            isv_prod_id: u16_at(ISV_PROD_ID_OFFSET),
            isv_svn: u16_at(ISV_SVN_OFFSET),
            config_svn: u16_at(CONFIG_SVN_OFFSET),
            debuggable: flags & SGX_FLAGS_DEBUG != 0,
            report_data: quote[REPORT_DATA_OFFSET..REPORT_DATA_OFFSET + 64]
                .try_into()
                .unwrap(),
        })
    }
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "  |- mr_enclave     :  {}", hex(&self.mr_enclave))?;
        writeln!(f, "  |- mr_signer      :  {}", hex(&self.mr_signer))?;
        writeln!(f, "  |- isv_prod_id    :  {}", self.isv_prod_id)?;
        writeln!(f, "  |- isv_svn        :  {}", self.isv_svn)?;
        writeln!(f, "  |- config_svn     :  {}", self.config_svn)?;
        write!(f, "  |- debuggable     :  {}", self.debuggable)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    let s = s.as_bytes();
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (byte, pair) in out.iter_mut().zip(s.chunks(2)) {
        let digit = |c: u8| (c as char).to_digit(16);
        *byte = (digit(pair[0])? * 16 + digit(pair[1])?) as u8;
    }
    Some(out)
}

/// One set of measurements the peer is allowed to have, as in the
/// `sgx_mrs` array of the RA configuration
#[derive(Deserialize, Debug, Clone)]
pub struct MRsValue {
    pub mr_enclave: String,
    pub mr_signer: String,
    pub isv_prod_id: u32,
    pub isv_svn: u32,
    pub config_svn: u32,
    pub debuggable: bool,
}

/// The `ra_config` of `init_ra_conf.json`
#[derive(Deserialize, Debug, Clone)]
pub struct RAConfig {
    pub verify_mr_enclave: String,
    pub verify_mr_signer: String,
    pub verify_isv_prod_id: String,
    pub verify_isv_svn: String,
    pub verify_config_svn: String,
    pub verify_enclave_debuggable: String,
    pub sgx_mrs: Vec<MRsValue>,
}

struct AllowedMRs {
    mr_enclave: Option<[u8; 32]>,
    mr_signer: Option<[u8; 32]>,
    isv_prod_id: u32,
    isv_svn: u32,
    config_svn: u32,
    debuggable: bool,
}

/// Which measurements of the peer to check and the values allowed
pub struct Policy {
    verify_mr_enclave: bool,
    verify_mr_signer: bool,
    verify_isv_prod_id: bool,
    verify_isv_svn: bool,
    verify_config_svn: bool,
    verify_enclave_debuggable: bool,
    allowed: Vec<AllowedMRs>,
}

impl Policy {
    pub fn from_config(config: &RAConfig) -> Result<Self> {
        let on = |v: &str| v == "on";
        let verify_mr_enclave = on(&config.verify_mr_enclave);
        let verify_mr_signer = on(&config.verify_mr_signer);

        let mut allowed = Vec::new();
        for (i, mrs) in config.sgx_mrs.iter().enumerate() {
            let mr_enclave = parse_hex32(&mrs.mr_enclave);
            let mr_signer = parse_hex32(&mrs.mr_signer);
            if verify_mr_enclave && mr_enclave.is_none() {
                return Err(ra_err!(
                    "sgx_mrs[{}].mr_enclave is not 32 bytes in hex, but mr_enclave is verified",
                    i
                ));
            }
            if verify_mr_signer && mr_signer.is_none() {
                return Err(ra_err!(
                    "sgx_mrs[{}].mr_signer is not 32 bytes in hex, but mr_signer is verified",
                    i
                ));
            }
            allowed.push(AllowedMRs {
                mr_enclave,
                mr_signer,
                isv_prod_id: mrs.isv_prod_id,
                isv_svn: mrs.isv_svn,
                config_svn: mrs.config_svn,
                debuggable: mrs.debuggable,
            });
        }

        Ok(Self {
            verify_mr_enclave,
            verify_mr_signer,
            verify_isv_prod_id: on(&config.verify_isv_prod_id),
            verify_isv_svn: on(&config.verify_isv_svn),
            verify_config_svn: on(&config.verify_config_svn),
            verify_enclave_debuggable: on(&config.verify_enclave_debuggable),
            allowed,
        })
    }

    /// The peer is accepted if it matches one of the allowed sets on every
    /// measurement to verify. With no set at all, no peer is accepted.
    pub fn allows(&self, m: &Measurement) -> bool {
        self.allowed.iter().any(|a| {
            (!self.verify_mr_enclave || a.mr_enclave == Some(m.mr_enclave))
                && (!self.verify_mr_signer || a.mr_signer == Some(m.mr_signer))
                && (!self.verify_isv_prod_id || a.isv_prod_id == u32::from(m.isv_prod_id))
                && (!self.verify_isv_svn || a.isv_svn == u32::from(m.isv_svn))
                && (!self.verify_config_svn || a.config_svn == u32::from(m.config_svn))
                && (!self.verify_enclave_debuggable || a.debuggable == m.debuggable)
        })
    }
}

fn spki_hash(spki: &[u8]) -> [u8; 64] {
    let mut report_data = [0u8; 64];
    report_data[..32].copy_from_slice(digest::digest(&digest::SHA256, spki).as_ref());
    report_data
}

/// A fresh key pair and a self-signed certificate for it, with the quote of
/// this enclave binding the key
pub struct Identity {
    pub cert: CertificateDer<'static>,
    pub key: PrivatePkcs8KeyDer<'static>,
}

impl Identity {
    pub fn generate(attestation: &dyn Attestation) -> Result<Self> {
        let key_pair = KeyPair::generate()?;
        let report_data = spki_hash(&key_pair.subject_public_key_info());
        let quote = attestation.generate_quote(&report_data)?;

        // rcgen puts the content as it is in the extension, as OpenSSL does
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(&QUOTE_OID, quote));
        let cert = params.self_signed(&key_pair)?;

        Ok(Self {
            cert: cert.der().clone(),
            key: PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
        })
    }
}

/// Check the certificate of the peer: the quote it carries must be valid and
/// bound to the certificate, and its measurements allowed. Returns the SPKI
/// of the certificate, which is the key the peer proves possession of.
pub(crate) fn verify_peer_cert(
    cert_der: &[u8],
    attestation: &dyn Attestation,
    policy: &Policy,
) -> Result<Vec<u8>> {
    let (_, cert) = X509Certificate::from_der(cert_der)
        .map_err(|e| ra_err!("cannot parse the certificate of the peer: {}", e))?;

    let oid = Oid::from(&QUOTE_OID).unwrap();
    let ext = cert
        .get_extension_unique(&oid)
        .map_err(|e| ra_err!("bad extensions in the certificate of the peer: {}", e))?
        .ok_or_else(|| ra_err!("the certificate of the peer has no quote"))?;
    let quote = ext.value;

    attestation
        .verify_quote(quote)
        .map_err(|e| ra_err!("verify quote failed: {}", e))?;
    let measurement = Measurement::from_quote(quote)?;

    let spki = cert.tbs_certificate.subject_pki.raw;
    if measurement.report_data[..32] != spki_hash(spki)[..32] {
        return Err(ra_err!(
            "the quote of the peer does not belong to the public key of its certificate"
        ));
    }

    if !policy.allows(&measurement) {
        return Err(ra_err!(
            "the SGX measurements of the peer are not in the allowable list\n{}",
            measurement
        ));
    }
    println!("RA-TLS: verified the peer\n{}", measurement);
    Ok(spki.to_vec())
}

fn rejected(e: Box<dyn Error + Send + Sync>) -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::Other(OtherError(Arc::new(RaError(
        e.to_string(),
    )))))
}

#[derive(Debug)]
struct RaVerifier {
    attestation: Arc<dyn Attestation>,
    policy: Policy,
    algorithms: WebPkiSupportedAlgorithms,
}

// Policy has no Debug, as it holds nothing of interest
impl fmt::Debug for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Policy").finish_non_exhaustive()
    }
}

impl fmt::Debug for dyn Attestation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Attestation")
    }
}

impl ServerCertVerifier for RaVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        verify_peer_cert(end_entity, self.attestation.as_ref(), &self.policy)
            .map(|_| ServerCertVerified::assertion())
            .map_err(rejected)
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        // Only TLS 1.3 is enabled
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        // The signature is checked against the key the certificate carries.
        // That the key is the one of an attested enclave was checked with the
        // certificate. The raw key is used since the certificate is not
        // issued by anyone and is not made for the PKI libraries.
        let (_, parsed) = X509Certificate::from_der(cert)
            .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
        let spki = SubjectPublicKeyInfoDer::from(parsed.tbs_certificate.subject_pki.raw);
        verify_tls13_signature_with_raw_key(message, &spki, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// The TLS configuration of an RA-TLS client: it presents a certificate with
/// our own quote and only accepts servers which are attested and match the
/// policy.
pub fn client_config(
    attestation: Arc<dyn Attestation>,
    policy: Policy,
) -> Result<Arc<ClientConfig>> {
    let identity = Identity::generate(attestation.as_ref())?;
    let provider = Arc::new(ring_provider::default_provider());
    let verifier = RaVerifier {
        attestation,
        policy,
        algorithms: provider.signature_verification_algorithms,
    };

    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_client_auth_cert(vec![identity.cert], identity.key.into())?;
    config.alpn_protocols = vec![ALPN_H2.to_vec()];
    Ok(Arc::new(config))
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// A quote which is not one, with measurements as given by the test
    pub struct FakeAttestation {
        pub measurement: Measurement,
        pub verify_result: std::result::Result<QuoteStatus, String>,
    }

    impl FakeAttestation {
        pub fn new() -> Self {
            Self {
                measurement: Measurement {
                    mr_enclave: [0x11; 32],
                    mr_signer: [0x22; 32],
                    isv_prod_id: 3,
                    isv_svn: 4,
                    config_svn: 5,
                    debuggable: true,
                    report_data: [0; 64],
                },
                verify_result: Ok(QuoteStatus::Ok),
            }
        }
    }

    impl Attestation for FakeAttestation {
        fn generate_quote(&self, report_data: &[u8; 64]) -> Result<Vec<u8>> {
            let m = &self.measurement;
            let mut quote = vec![0u8; QUOTE_HEADER_LEN + REPORT_BODY_LEN + 4 + 100];
            quote[0] = 3;
            let flags: u64 = if m.debuggable { SGX_FLAGS_DEBUG } else { 0 } | 0x4;
            quote[FLAGS_OFFSET..FLAGS_OFFSET + 8].copy_from_slice(&flags.to_le_bytes());
            quote[MR_ENCLAVE_OFFSET..MR_ENCLAVE_OFFSET + 32].copy_from_slice(&m.mr_enclave);
            quote[MR_SIGNER_OFFSET..MR_SIGNER_OFFSET + 32].copy_from_slice(&m.mr_signer);
            quote[ISV_PROD_ID_OFFSET..][..2].copy_from_slice(&m.isv_prod_id.to_le_bytes());
            quote[ISV_SVN_OFFSET..][..2].copy_from_slice(&m.isv_svn.to_le_bytes());
            quote[CONFIG_SVN_OFFSET..][..2].copy_from_slice(&m.config_svn.to_le_bytes());
            quote[REPORT_DATA_OFFSET..][..64].copy_from_slice(report_data);
            Ok(quote)
        }

        fn verify_quote(&self, _quote: &[u8]) -> Result<QuoteStatus> {
            self.verify_result.clone().map_err(|e| ra_err!("{}", e))
        }
    }

    pub fn config(mr_enclave: &str) -> RAConfig {
        RAConfig {
            verify_mr_enclave: "on".into(),
            verify_mr_signer: "off".into(),
            verify_isv_prod_id: "off".into(),
            verify_isv_svn: "off".into(),
            verify_config_svn: "off".into(),
            verify_enclave_debuggable: "off".into(),
            sgx_mrs: vec![MRsValue {
                mr_enclave: mr_enclave.into(),
                mr_signer: "".into(),
                isv_prod_id: 0,
                isv_svn: 0,
                config_svn: 0,
                debuggable: false,
            }],
        }
    }

    fn identity_cert(att: &FakeAttestation) -> Vec<u8> {
        Identity::generate(att).unwrap().cert.to_vec()
    }

    #[test]
    fn quote_fields_round_trip() {
        let att = FakeAttestation::new();
        let report_data = [7u8; 64];
        let quote = att.generate_quote(&report_data).unwrap();
        let m = Measurement::from_quote(&quote).unwrap();
        assert_eq!(m.mr_enclave, [0x11; 32]);
        assert_eq!(m.mr_signer, [0x22; 32]);
        assert_eq!((m.isv_prod_id, m.isv_svn, m.config_svn), (3, 4, 5));
        assert!(m.debuggable);
        assert_eq!(m.report_data, report_data);
    }

    #[test]
    fn quote_must_be_an_sgx_quote() {
        let att = FakeAttestation::new();
        let quote = att.generate_quote(&[0; 64]).unwrap();
        assert!(Measurement::from_quote(&quote[..400]).is_err());
        let mut tdx = quote.clone();
        tdx[0] = 4;
        tdx[4] = 0x81;
        assert!(Measurement::from_quote(&tdx).is_err());
        let mut v2 = quote;
        v2[0] = 2;
        assert!(Measurement::from_quote(&v2).is_err());
    }

    #[test]
    fn hex_parsing() {
        assert_eq!(parse_hex32(&"ab".repeat(32)), Some([0xab; 32]));
        assert_eq!(parse_hex32(&"AB".repeat(32)), Some([0xab; 32]));
        assert_eq!(parse_hex32(&"ab".repeat(31)), None);
        assert_eq!(parse_hex32(&"ag".repeat(32)), None);
        assert_eq!(parse_hex32(""), None);
    }

    #[test]
    fn policy_matches_only_the_verified_fields() {
        let att = FakeAttestation::new();
        let m = att.measurement.mr_enclave;
        let mut config = config(&hex(&m));
        let measurement = |att: &FakeAttestation| {
            Measurement::from_quote(&att.generate_quote(&[0; 64]).unwrap()).unwrap()
        };
        assert!(Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&att)));

        // The enclave is another one
        let mut other = FakeAttestation::new();
        other.measurement.mr_enclave = [0x12; 32];
        assert!(!Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&other)));

        // Not verified, but the other fields differ from the allowed ones
        config.verify_mr_enclave = "off".into();
        assert!(Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&other)));

        // The debug flag
        config.verify_enclave_debuggable = "on".into();
        assert!(!Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&att)));
        config.sgx_mrs[0].debuggable = true;
        assert!(Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&att)));

        // One of several sets is enough
        config.verify_isv_svn = "on".into();
        config.sgx_mrs.push(MRsValue {
            isv_svn: 4,
            debuggable: true,
            ..config.sgx_mrs[0].clone()
        });
        assert!(Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&att)));

        // Nothing is allowed without any set
        config.sgx_mrs.clear();
        assert!(!Policy::from_config(&config)
            .unwrap()
            .allows(&measurement(&att)));
    }

    #[test]
    fn policy_requires_valid_hex_of_the_verified_fields() {
        assert!(Policy::from_config(&config("")).is_err());
        let mut c = config("");
        c.verify_mr_enclave = "off".into();
        assert!(Policy::from_config(&c).is_ok());
        c.verify_mr_signer = "on".into();
        assert!(Policy::from_config(&c).is_err());
    }

    #[test]
    fn the_certificate_carries_the_quote_bound_to_its_key() {
        let att = FakeAttestation::new();
        let cert = identity_cert(&att);
        let policy = Policy::from_config(&config(&hex(&att.measurement.mr_enclave))).unwrap();
        let spki = verify_peer_cert(&cert, &att, &policy).unwrap();

        let (_, parsed) = X509Certificate::from_der(&cert).unwrap();
        assert_eq!(spki, parsed.tbs_certificate.subject_pki.raw);

        // The quote is the raw value of the extension
        let oid = Oid::from(&QUOTE_OID).unwrap();
        let ext = parsed.get_extension_unique(&oid).unwrap().unwrap();
        assert!(!ext.critical);
        let m = Measurement::from_quote(ext.value).unwrap();
        assert_eq!(m.report_data, spki_hash(&spki));
    }

    #[test]
    fn a_quote_of_another_key_is_rejected() {
        let att = FakeAttestation::new();
        let policy = Policy::from_config(&config(&hex(&att.measurement.mr_enclave))).unwrap();

        // Take the quote of one certificate and put it into the one of another key
        let quote = att.generate_quote(&[9; 64]).unwrap();
        let key_pair = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(&QUOTE_OID, quote));
        let cert = params.self_signed(&key_pair).unwrap();
        let err = verify_peer_cert(cert.der(), &att, &policy).unwrap_err();
        assert!(err.to_string().contains("does not belong"), "{}", err);
    }

    #[test]
    fn a_certificate_without_quote_is_rejected() {
        let att = FakeAttestation::new();
        let policy = Policy::from_config(&config(&hex(&att.measurement.mr_enclave))).unwrap();
        let key_pair = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(Vec::<String>::new())
            .unwrap()
            .self_signed(&key_pair)
            .unwrap();
        let err = verify_peer_cert(cert.der(), &att, &policy).unwrap_err();
        assert!(err.to_string().contains("no quote"), "{}", err);
        assert!(verify_peer_cert(b"not a certificate", &att, &policy).is_err());
    }

    #[test]
    fn measurements_not_allowed_and_failed_verification_are_rejected() {
        let mut att = FakeAttestation::new();
        let cert = identity_cert(&att);
        let policy = Policy::from_config(&config(&hex(&[0x99; 32]))).unwrap();
        let err = verify_peer_cert(&cert, &att, &policy).unwrap_err();
        assert!(
            err.to_string().contains("not in the allowable list"),
            "{}",
            err
        );

        let policy = Policy::from_config(&config(&hex(&att.measurement.mr_enclave))).unwrap();
        att.verify_result = Err("revoked".into());
        let err = verify_peer_cert(&cert, &att, &policy).unwrap_err();
        assert!(err.to_string().contains("revoked"), "{}", err);
        att.verify_result = Ok(QuoteStatus::NeedsAttention(QV_RESULT_OUT_OF_DATE));
        assert!(verify_peer_cert(&cert, &att, &policy).is_ok());
    }
}
