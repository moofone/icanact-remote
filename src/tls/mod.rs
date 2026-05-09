pub mod name;
pub mod resolver;

use crate::{NodeId, Result, SecretKey};
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

pub const ALPN_ICANACT_V3: &[u8] = b"icanact-remote-v3";

pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub struct TlsConfig {
    pub secret_key: SecretKey,
    pub node_id: NodeId,
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
    config.alpn_protocols = vec![ALPN_ICANACT_V3.to_vec()];
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
    config.alpn_protocols = vec![ALPN_ICANACT_V3.to_vec()];
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
                        "NodeId mismatch: expected {}, got {}",
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

pub fn extract_node_id_from_cert(cert: &CertificateDer<'_>) -> std::result::Result<NodeId, Error> {
    let cert_bytes = cert.as_ref();
    let ed25519_oid_pattern = &[0x06, 0x03, 0x2B, 0x65, 0x70];

    let mut oid_positions = Vec::new();
    for i in 0..cert_bytes.len().saturating_sub(ed25519_oid_pattern.len()) {
        if &cert_bytes[i..i + ed25519_oid_pattern.len()] == ed25519_oid_pattern {
            oid_positions.push(i);
        }
    }
    if oid_positions.is_empty() {
        return Err(Error::General(
            "Certificate does not contain Ed25519 OID".into(),
        ));
    }

    let mut public_key_bytes = None;
    for oid_index in oid_positions {
        let search_start = oid_index + ed25519_oid_pattern.len();
        for i in search_start..cert_bytes.len().saturating_sub(2).min(search_start + 10) {
            if cert_bytes[i] == 0x03 {
                let length = cert_bytes[i + 1] as usize;
                if length == 33 {
                    let key_start = i + 3;
                    let key_end = key_start + 32;
                    if key_end <= cert_bytes.len() {
                        public_key_bytes = Some(&cert_bytes[key_start..key_end]);
                        break;
                    }
                }
            }
        }
        if public_key_bytes.is_some() {
            break;
        }
    }

    let key_bytes = public_key_bytes
        .ok_or_else(|| Error::General("Could not find Ed25519 public key in certificate".into()))?;

    NodeId::from_bytes(key_bytes)
        .map_err(|e| Error::General(format!("Invalid public key in certificate: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_cert_verifier_requires_authenticated_clients() {
        let verifier = NodeIdClientVerifier::new();

        assert!(verifier.client_auth_mandatory());
    }
}
