//! Transport for a DCC connection: plain TCP, or TLS for the secure variants.
//!
//! Secure DCC has no PKI. Peers present self-signed certificates they generate
//! per session, exactly as mIRC and HexChat do, so certificate *validation* is
//! impossible by construction. What we can do — and do — is accept the
//! self-signed certificate and hand its SHA-256 fingerprint up to the UI, so
//! the two humans can compare it out of band if they care. This buys
//! confidentiality against a passive observer, not authentication.

use std::sync::{Arc, Mutex};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::DccError;

/// Anything we can read and write a DCC session over.
pub trait DccIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> DccIo for T {}

pub struct DccStream {
    pub io: Box<dyn DccIo>,
    /// SHA-256 of the peer's leaf certificate, hex, colon-separated.
    ///
    /// Only populated on the connecting side: the listening side would need to
    /// demand client authentication to see a peer certificate, and no DCC
    /// client offers one.
    pub fingerprint: Option<String>,
}

fn fingerprint_of(cert: &[u8]) -> String {
    let digest = Sha256::digest(cert);
    digest
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Certificate verifier that accepts any peer certificate and records it.
///
/// Safe only because DCC peer certificates are unverifiable in principle (see
/// the module docs) and the fingerprint is always surfaced to the user. Never
/// reuse this for a connection where a real trust chain exists.
#[derive(Debug)]
struct RecordingVerifier {
    captured: Arc<Mutex<Option<Vec<u8>>>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for RecordingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if let Ok(mut slot) = self.captured.lock() {
            *slot = Some(end_entity.as_ref().to_vec());
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Wrap an accepted socket as the TLS server side, using a fresh self-signed
/// certificate for this session.
pub async fn accept_tls(tcp: TcpStream) -> Result<DccStream, DccError> {
    let certified = rcgen::generate_simple_self_signed(vec!["dcc.local".to_string()])
        .map_err(|e| DccError::Tls(e.to_string()))?;

    let cert_der = CertificateDer::from(certified.cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(certified.signing_key.serialize_der())
        .map_err(|e| DccError::Tls(e.to_string()))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| DccError::Tls(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|e| DccError::Tls(e.to_string()))?;

    let acceptor = TlsAcceptor::from(Arc::new(config));
    let tls = acceptor
        .accept(tcp)
        .await
        .map_err(|e| DccError::Tls(e.to_string()))?;

    Ok(DccStream {
        io: Box::new(tls),
        fingerprint: None,
    })
}

/// Wrap a dialled socket as the TLS client side, capturing the peer's
/// certificate fingerprint.
pub async fn connect_tls(tcp: TcpStream) -> Result<DccStream, DccError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| DccError::Tls(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(RecordingVerifier {
            captured: captured.clone(),
            provider,
        }))
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    // The peer certificate is self-signed and its SAN is meaningless, so the
    // name here is a placeholder the verifier ignores.
    let server_name = ServerName::try_from("dcc.local")
        .map_err(|e| DccError::Tls(e.to_string()))?
        .to_owned();

    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| DccError::Tls(e.to_string()))?;

    let fingerprint = captured
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
        .map(|cert| fingerprint_of(&cert));

    Ok(DccStream {
        io: Box::new(tls),
        fingerprint,
    })
}

pub fn plain(tcp: TcpStream) -> DccStream {
    DccStream {
        io: Box::new(tcp),
        fingerprint: None,
    }
}

#[cfg(test)]
mod tests {
    use super::fingerprint_of;

    #[test]
    fn fingerprint_is_uppercase_colon_separated_sha256() {
        let fingerprint = fingerprint_of(b"");
        // SHA-256 of the empty input, the standard test vector.
        assert!(fingerprint.starts_with("E3:B0:C4:42:98:FC:1C:14"));
        assert_eq!(fingerprint.len(), 32 * 3 - 1);
    }
}
