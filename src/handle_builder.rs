use crate::transport::RegistryTransportBootstrap;
use crate::{GossipConfig, Result};

#[derive(Debug, Clone, Copy, Default)]
pub struct BuilderTlsBootstrap;

impl RegistryTransportBootstrap for BuilderTlsBootstrap {
    fn stack_name(&self) -> &'static str {
        "builder+tls"
    }

    fn prepare_config(
        &self,
        secret_key: &crate::SecretKey,
        config: &mut GossipConfig,
    ) -> Result<()> {
        let derived_keypair = secret_key.to_keypair();
        match config.key_pair.as_ref() {
            Some(existing) => {
                if existing.peer_id() != derived_keypair.peer_id() {
                    return Err(crate::GossipError::InvalidKeyPair(
                        "GossipConfig.key_pair does not match TLS secret key".to_string(),
                    ));
                }
            }
            None => {
                config.key_pair = Some(derived_keypair);
            }
        }
        Ok(())
    }

    fn configure_registry(
        &self,
        registry: &mut crate::registry::GossipRegistry,
        secret_key: crate::SecretKey,
    ) -> Result<()> {
        registry.enable_tls(secret_key)
    }
}
