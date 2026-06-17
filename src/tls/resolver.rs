use crate::{Result, SecretKey};
use rustls::client::ResolvesClientCert;
use rustls::pki_types::CertificateDer;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::{CertifiedKey, Signer, SigningKey};
use rustls::{Error as TlsError, SignatureAlgorithm, SignatureScheme};
use std::sync::Arc;

#[derive(Debug)]
pub struct AlwaysResolvesCert {
    certified_key: Arc<CertifiedKey>,
}

impl AlwaysResolvesCert {
    pub fn new(secret_key: &SecretKey) -> Result<Self> {
        let cert = generate_self_signed_cert(secret_key)?;
        let signing_key = Ed25519SigningKey::new(secret_key.clone());
        let certified_key = Arc::new(CertifiedKey::new(vec![cert], Arc::new(signing_key)));
        Ok(Self { certified_key })
    }
}

impl ResolvesServerCert for AlwaysResolvesCert {
    fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.clone())
    }
}

impl ResolvesClientCert for AlwaysResolvesCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct Ed25519SigningKey {
    secret_key: SecretKey,
}

impl Ed25519SigningKey {
    fn new(secret_key: SecretKey) -> Self {
        Self { secret_key }
    }
}

impl SigningKey for Ed25519SigningKey {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ED25519
    }

    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
        if offered.contains(&SignatureScheme::ED25519) {
            Some(Box::new(self.clone()))
        } else {
            None
        }
    }
}

impl Signer for Ed25519SigningKey {
    fn sign(&self, message: &[u8]) -> std::result::Result<Vec<u8>, TlsError> {
        let signature = self.secret_key.sign(message);
        Ok(signature.to_bytes().to_vec())
    }

    fn scheme(&self) -> SignatureScheme {
        SignatureScheme::ED25519
    }
}

fn generate_self_signed_cert(secret_key: &SecretKey) -> Result<CertificateDer<'static>> {
    let public_key = secret_key.public();
    let public_key_bytes = public_key.as_bytes();

    let mut tbs_cert = Vec::new();
    tbs_cert.extend_from_slice(&[0xA0, 0x03, 0x02, 0x01, 0x02]);
    tbs_cert.extend_from_slice(&[0x02, 0x01, 0x01]);
    tbs_cert.extend_from_slice(&[0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70]);
    tbs_cert.extend_from_slice(&[
        0x30, 0x0F, 0x31, 0x0D, 0x30, 0x0B, 0x06, 0x03, 0x55, 0x04, 0x03, 0x0C, 0x04, 0x6E, 0x6F,
        0x64, 0x65,
    ]);
    // Validity. Use GeneralizedTime (tag 0x18, 15 bytes "YYYYMMDDHHMMSSZ") so
    // notAfter can be the RFC 5280 §4.1.2.5 "no well-defined expiration date"
    // sentinel 99991231235959Z. UTCTime (the previous encoding) cannot
    // represent years past 2049 and pinned a hard 2034-01-01 cluster-wide TLS
    // time-bomb. Authenticity here comes from Ed25519 key possession, not a
    // CA/expiry trust model, so these certs are intended never to expire.
    // Validity SEQUENCE body = (2 + 15) + (2 + 15) = 34 = 0x22 bytes.
    tbs_cert.extend_from_slice(&[0x30, 0x22, 0x18, 0x0F]);
    tbs_cert.extend_from_slice(b"20240101000000Z");
    tbs_cert.extend_from_slice(&[0x18, 0x0F]);
    tbs_cert.extend_from_slice(b"99991231235959Z");
    tbs_cert.extend_from_slice(&[
        0x30, 0x0F, 0x31, 0x0D, 0x30, 0x0B, 0x06, 0x03, 0x55, 0x04, 0x03, 0x0C, 0x04, 0x6E, 0x6F,
        0x64, 0x65,
    ]);
    tbs_cert.extend_from_slice(&[
        0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00,
    ]);
    tbs_cert.extend_from_slice(public_key_bytes);

    let tbs_len = tbs_cert.len();
    let mut tbs_wrapped = vec![0x30, 0x81, tbs_len as u8];
    tbs_wrapped.extend_from_slice(&tbs_cert);

    let signature = secret_key.sign(&tbs_wrapped);

    let mut cert = Vec::new();
    cert.extend_from_slice(&[0x30, 0x82, 0x00, 0x00]);
    cert.extend_from_slice(&tbs_wrapped);
    cert.extend_from_slice(&[0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70]);
    cert.extend_from_slice(&[0x03, 0x41, 0x00]);
    cert.extend_from_slice(&signature.to_bytes());

    let total_len = cert.len() - 4;
    cert[2] = ((total_len >> 8) & 0xFF) as u8;
    cert[3] = (total_len & 0xFF) as u8;

    Ok(CertificateDer::from(cert))
}

/// Test-only access to the self-signed certificate builder so sibling TLS
/// modules can exercise the server-cert verifier against real certs.
#[cfg(test)]
pub(crate) fn test_self_signed_cert(secret_key: &SecretKey) -> Result<CertificateDer<'static>> {
    generate_self_signed_cert(secret_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecretKey;

    fn der_contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    /// Audit finding A4: the self-signed certificate hardcoded a notAfter of
    /// 2034-01-01 as a UTCTime, and both TLS verifiers ignore certificate
    /// validity. On that date every cluster TLS handshake would begin failing.
    /// These are pinned-key certs (authenticity = Ed25519 key possession, not a
    /// CA/expiry model), so the cert must carry the RFC 5280 §4.1.2.5
    /// "no well-defined expiration" sentinel (99991231235959Z) and never expire.
    #[test]
    fn self_signed_cert_has_no_near_term_expiry() {
        let key = SecretKey::generate();
        let cert = generate_self_signed_cert(&key).expect("cert generation should succeed");
        let der = cert.as_ref();
        assert!(
            !der_contains(der, b"340101000000Z"),
            "certificate must not embed the old 2034 UTCTime expiry time-bomb"
        );
        assert!(
            der_contains(der, b"99991231235959Z"),
            "certificate notAfter must be the far-future GeneralizedTime sentinel"
        );
    }
}
