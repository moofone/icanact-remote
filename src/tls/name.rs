use crate::{NodeId, PublicKey};
use data_encoding::BASE32_DNSSEC;

/// Encode a NodeId as a DNS-safe name.
pub fn encode(node_id: &NodeId) -> String {
    let encoded = BASE32_DNSSEC.encode(node_id.as_bytes());
    format!("{}.icanact.invalid", encoded.to_lowercase())
}

/// Decode a DNS name back to NodeId.
pub fn decode(name: &str) -> Option<NodeId> {
    let node_part = if let Some(stripped) = name.strip_suffix(".icanact.invalid") {
        stripped
    } else if let Some(first_part) = name.split('.').next() {
        first_part
    } else {
        return None;
    };

    let bytes = BASE32_DNSSEC
        .decode(node_part.to_uppercase().as_bytes())
        .ok()?;

    PublicKey::from_bytes(&bytes).ok()
}
