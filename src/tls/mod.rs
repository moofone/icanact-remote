pub mod name;
pub mod resolver;

use crate::{GossipNodeId, Result, SecretKey};
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::version::TLS13;
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, Error, ServerConfig, SignatureScheme,
};
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use x509_parser::prelude::{FromDer, X509Certificate};

pub const ALPN_ICANACT_V4: &[u8] = b"icanact-remote-v4";

pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub struct TlsConfig {
    pub secret_key: SecretKey,
    pub node_id: GossipNodeId,
    pub client_config: Arc<ClientConfig>,
    pub server_config: Arc<ServerConfig>,
}

impl TlsConfig {
    pub fn new(secret_key: SecretKey) -> Result<Self> {
        Self::with_peer_discovery(secret_key, false)
    }

    pub fn with_peer_discovery(secret_key: SecretKey, enable_peer_discovery: bool) -> Result<Self> {
        ensure_crypto_provider();
        let node_id = secret_key.public();
        let client_config = make_client_config(&secret_key, enable_peer_discovery)?;
        let server_config = make_server_config(&secret_key, enable_peer_discovery)?;
        Ok(Self {
            secret_key,
            node_id,
            client_config: Arc::new(client_config),
            server_config: Arc::new(server_config),
        })
    }

    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.client_config.clone())
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config.clone())
    }
}

fn make_client_config(
    secret_key: &SecretKey,
    _enable_peer_discovery: bool,
) -> Result<ClientConfig> {
    let mut config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NodeIdServerVerifier::new()))
        .with_client_cert_resolver(Arc::new(resolver::AlwaysResolvesCert::new(secret_key)?));
    config.alpn_protocols = vec![ALPN_ICANACT_V4.to_vec()];
    config.resumption = Resumption::disabled();
    config.cert_compressors.clear();
    config.cert_decompressors.clear();
    config.max_fragment_size = None;
    if std::env::var("SSLKEYLOGFILE").is_ok() {
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }
    Ok(config)
}

fn make_server_config(
    secret_key: &SecretKey,
    _enable_peer_discovery: bool,
) -> Result<ServerConfig> {
    let mut config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
        .with_client_cert_verifier(Arc::new(NodeIdClientVerifier::new()))
        .with_cert_resolver(Arc::new(resolver::AlwaysResolvesCert::new(secret_key)?));
    config.alpn_protocols = vec![ALPN_ICANACT_V4.to_vec()];
    config.send_tls13_tickets = 0;
    config.cert_compressors.clear();
    config.cert_decompressors.clear();
    config.max_fragment_size = None;
    if std::env::var("SSLKEYLOGFILE").is_ok() {
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }
    Ok(config)
}

#[derive(Debug)]
struct NodeIdServerVerifier;

impl NodeIdServerVerifier {
    fn new() -> Self {
        Self
    }
}

impl ServerCertVerifier for NodeIdServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, Error> {
        let actual_node_id = extract_node_id_from_cert(end_entity)?;
        if let ServerName::DnsName(dns_name) = server_name {
            if let Some(expected_node_id) = name::decode(dns_name.as_ref()) {
                if actual_node_id != expected_node_id {
                    return Err(Error::General(format!(
                        "GossipNodeId mismatch: expected {}, got {}",
                        expected_node_id.fmt_short(),
                        actual_node_id.fmt_short()
                    )));
                }
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        let node_id = extract_node_id_from_cert(cert)?;
        let signature = ed25519_dalek::Signature::from_slice(dss.signature())
            .map_err(|e| Error::General(format!("Invalid signature: {}", e)))?;
        node_id
            .verify(message, &signature)
            .map_err(|e| Error::General(format!("Signature verification failed: {}", e)))?;
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

#[derive(Debug)]
struct NodeIdClientVerifier;

impl NodeIdClientVerifier {
    fn new() -> Self {
        Self
    }
}

impl ClientCertVerifier for NodeIdClientVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, Error> {
        let _node_id = extract_node_id_from_cert(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        let node_id = extract_node_id_from_cert(cert)?;
        let signature = ed25519_dalek::Signature::from_slice(dss.signature())
            .map_err(|e| Error::General(format!("Invalid signature: {}", e)))?;
        node_id
            .verify(message, &signature)
            .map_err(|e| Error::General(format!("Signature verification failed: {}", e)))?;
        Ok(HandshakeSignatureValid::assertion())
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

pub fn extract_node_id_from_cert(
    cert: &CertificateDer<'_>,
) -> std::result::Result<GossipNodeId, Error> {
    let (_, parsed) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| Error::General(format!("Invalid certificate DER: {e}")))?;
    let public_key = parsed.public_key();
    if public_key.algorithm.algorithm != x509_parser::oid_registry::OID_SIG_ED25519 {
        return Err(Error::General(
            "Certificate does not contain Ed25519 public key".into(),
        ));
    }
    let key_bytes = public_key.subject_public_key.data.as_ref();
    if key_bytes.len() != 32 {
        return Err(Error::General(format!(
            "Invalid Ed25519 public key length in certificate: expected 32, got {}",
            key_bytes.len()
        )));
    }
    GossipNodeId::from_bytes(key_bytes)
        .map_err(|e| Error::General(format!("Invalid public key in certificate: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now() -> UnixTime {
        UnixTime::since_unix_epoch(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
    }

    #[test]
    fn client_cert_verifier_requires_authenticated_clients() {
        let verifier = NodeIdClientVerifier::new();

        assert!(verifier.client_auth_mandatory());
    }

    #[test]
    fn server_verifier_accepts_matching_pinned_node_id() {
        let key = SecretKey::generate();
        let node_id = key.public();
        let cert = resolver::test_self_signed_cert(&key).expect("cert");

        let verifier = NodeIdServerVerifier::new();
        let pinned = ServerName::try_from(name::encode(&node_id)).unwrap();
        let res = verifier.verify_server_cert(&cert, &[], &pinned, &[], now());
        assert!(
            res.is_ok(),
            "matching pinned GossipNodeId must be accepted: {res:?}"
        );
    }

    #[test]
    fn server_verifier_rejects_mismatched_pinned_node_id() {
        let key = SecretKey::generate();
        let cert = resolver::test_self_signed_cert(&key).expect("cert");

        // Pin a DIFFERENT node id than the cert actually carries.
        let other_node_id = SecretKey::generate().public();
        let verifier = NodeIdServerVerifier::new();
        let pinned = ServerName::try_from(name::encode(&other_node_id)).unwrap();
        let res = verifier.verify_server_cert(&cert, &[], &pinned, &[], now());
        assert!(
            res.is_err(),
            "mismatched pinned GossipNodeId must be rejected (identity binding)"
        );
    }

    #[test]
    fn server_verifier_placeholder_sni_is_tofu_learnable() {
        // Bootstrap dials use a placeholder SNI that does not decode to a GossipNodeId.
        // The verifier accepts (key-possession is still proven via the TLS
        // signature), and the TRUE identity is recoverable from the cert for
        // TOFU-binding by the caller (R2). This proves the learned identity is
        // the real cert GossipNodeId, not None.
        let key = SecretKey::generate();
        let node_id = key.public();
        let cert = resolver::test_self_signed_cert(&key).expect("cert");

        let placeholder = "peer-4446.icanact.invalid";
        assert!(
            name::decode(placeholder).is_none(),
            "placeholder SNI must NOT decode to a GossipNodeId"
        );
        let sni = ServerName::try_from(placeholder).unwrap();

        let verifier = NodeIdServerVerifier::new();
        assert!(
            verifier
                .verify_server_cert(&cert, &[], &sni, &[], now())
                .is_ok(),
            "placeholder-SNI bootstrap dial must still complete the handshake"
        );

        // The identity used for TOFU binding is the real cert GossipNodeId.
        let learned = extract_node_id_from_cert(&cert).expect("extract");
        assert_eq!(
            learned, node_id,
            "TOFU-learned identity must equal the cert's true GossipNodeId"
        );
    }

    #[test]
    fn tofu_node_id_maps_to_expected_peer_id() {
        // The PeerId derived from the TOFU-learned GossipNodeId must match the one a
        // pinned GossipNodeId would have produced, so subsequent per-message gossip
        // guards (which compare embedded_peer_id) are consistent.
        let key = SecretKey::generate();
        let node_id = key.public();
        let cert = resolver::test_self_signed_cert(&key).expect("cert");
        let learned = extract_node_id_from_cert(&cert).expect("extract");
        assert_eq!(
            crate::PeerId::from_public_key(&learned),
            crate::PeerId::from_public_key(&node_id),
        );
    }
}
