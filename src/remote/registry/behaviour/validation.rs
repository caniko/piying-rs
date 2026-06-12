impl Behaviour {
    fn validate_provider_registration(&mut self, provider: &kad::ProviderRecord) -> bool {
        #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
        let source = &provider.provider;

        // Should be valid UTF8
        let key_str = match std::str::from_utf8(provider.key.as_ref()) {
            Ok(s) => s,
            Err(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("invalid UTF-8 in provider key from {source}");
                return false;
            }
        };

        if !self.is_valid_actor_name(key_str) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid actor name in provider registration from {source}: {key_str}");
            return false;
        }

        if !self.is_valid_provider_record(provider) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid provider record from {source}");
            return false;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!("validated provider registration for {key_str} from {source}");
        true
    }

    fn is_valid_provider_record(&self, provider: &kad::ProviderRecord) -> bool {
        if let Some(expires) = provider.expires {
            // Must not be expired
            if expires < Instant::now() {
                #[cfg(feature = "tracing")]
                tracing::warn!("provider record is already expired");
                return false;
            }
        }

        true
    }

    fn validate_metadata_record(&mut self, source: &PeerId, record: &kad::Record) -> bool {
        // Validate key format: "{name}:meta:{peer_id}"
        let key_str = match str::from_utf8(record.key.as_ref()) {
            Ok(s) => s,
            Err(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("invalid UTF-8 in metadata record key from {source}");
                return false;
            }
        };

        let parts: Vec<_> = key_str.splitn(2, ":meta:").collect();
        if parts.len() != 2 {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid metadata key format from {source}: {key_str}");
            return false;
        }

        let actor_name = parts[0];
        let peer_id_str = parts[1];

        if !self.is_valid_actor_name(actor_name) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid actor name from {source}: {actor_name}");
            return false;
        }

        // Parse and validate peer ID from key
        let record_peer_id = match PeerId::from_str(peer_id_str) {
            Ok(id) => id,
            Err(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("invalid peer ID in metadata key from {source}: {peer_id_str}");
                return false;
            }
        };

        // Verify the peer_id in key matches the source (prevents impersonation)
        if record_peer_id != *source {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "peer ID mismatch: source {source} trying to register metadata for peer {record_peer_id}"
            );
            return false;
        }

        // Validate metadata format and size
        if !self.is_valid_metadata(&record.value) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid metadata format from {source}");
            return false;
        }

        // Disabled for now: what if the record gets received before the start providing message
        // Verify the source peer is actually providing this actor
        // let actor_key = kad::RecordKey::new(&actor_name);
        // let providers = self.kademlia.store_mut().providers(&actor_key);

        // let is_provider = providers
        //     .iter()
        //     .any(|provider_record| provider_record.provider == *source);

        // if !is_provider {
        //     #[cfg(feature = "tracing")]
        //     tracing::warn!(
        //         "peer {source} trying to register metadata for {actor_name} without being a provider"
        //     );
        //     return false;
        // }

        #[cfg(feature = "tracing")]
        tracing::debug!("validated metadata record for {actor_name} from {source}");

        true
    }

    fn is_valid_actor_name(&self, name: &str) -> bool {
        !name.is_empty()
    }

    fn is_valid_metadata(&self, data: &[u8]) -> bool {
        // Size check
        if data.len() > 64 * 1024 {
            // 64KB limit
            return false;
        }

        ActorRegistration::from_bytes(data).is_ok()
    }
}
