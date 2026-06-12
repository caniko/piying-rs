impl Behaviour {

    /// Creates a new registry behaviour.
    pub fn new(local_peer_id: PeerId) -> Self {
        let mut config = kad::Config::new(proto_name());

        // Faster lookups for responsive actor discovery
        config.set_query_timeout(Duration::from_secs(10)); // Default: 60s

        // Lower replication for efficiency while maintaining availability
        config.set_replication_factor(NonZero::new(5).unwrap()); // Default: 20

        // Shorter TTL since actors are more dynamic than files
        config.set_record_ttl(Some(Duration::from_secs(3600))); // 1 hour, Default: 36 hours

        // More frequent re-publication for dynamic actors
        config.set_publication_interval(Some(Duration::from_secs(1800))); // 30 minutes, Default: 24 hours

        // Filter records to prevent registry pollution
        config.set_record_filtering(StoreInserts::FilterBoth); // Default: Unfiltered

        let mut kademlia = kad::Behaviour::with_config(
            local_peer_id,
            kad::store::MemoryStore::new(local_peer_id),
            config,
        );
        kademlia.set_mode(Some(kad::Mode::Server));

        Behaviour {
            kademlia,
            local_peer_id,
            pending_peers: HashMap::new(),
            pending_events: VecDeque::new(),
            registration_queries: HashMap::new(),
            lookup_queries: HashMap::new(),
        }
    }

    /// Registers an actor in the distributed registry.
    ///
    /// This is a low-level method that directly interacts with the Kademlia DHT
    /// and generates events. Use `ActorRef::register` for higher-level registration
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to register the actor under
    /// * `registration` - The actor registration information
    ///
    /// # Returns
    ///
    /// The query ID for tracking the registration progress, or an error if
    /// registration fails immediately.
    pub fn register(
        &mut self,
        name: impl Into<Arc<str>>,
        registration: ActorRegistration<'static>,
    ) -> Result<kad::QueryId, kad::store::Error> {
        self.register_with_reply(name.into(), registration, None)
            .map_err(|(_, err)| err)
    }

    /// Cancels an ongoing actor registration.
    ///
    /// This is a low-level method. Returns `true` if the registration was found
    /// and cancelled, `false` if no registration with the given query ID exists.
    pub fn cancel_registration(&mut self, query_id: &kad::QueryId) -> bool {
        self.registration_queries.remove(query_id).is_some()
    }

    /// Unregisters an actor from the distributed registry.
    ///
    /// This is a low-level method that removes the actor from both the provider
    /// records and metadata storage in the Kademlia DHT.
    ///
    /// # Arguments
    ///
    /// * `name` - The name the actor was registered under
    pub fn unregister(&mut self, name: &str) {
        self.kademlia
            .stop_providing(&kad::RecordKey::new(&name.as_bytes()));
        let key = format!("{}:meta:{}", name, self.local_peer_id);
        self.kademlia
            .remove_record(&kad::RecordKey::from(key.into_bytes()));
    }

    /// Looks up actors by name in the distributed registry.
    ///
    /// This is a low-level method that queries the Kademlia DHT and generates
    /// events as actors are discovered. Use `RemoteActorRef::lookup` for
    /// higher-level lookups that don't emit events.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to search for
    ///
    /// # Returns
    ///
    /// The query ID for tracking the lookup progress.
    pub fn lookup(&mut self, name: impl Into<Arc<str>>) -> kad::QueryId {
        self.lookup_with_reply(name.into(), None)
    }

    /// Looks up an actor in the local registry only.
    ///
    /// This is a low-level method that checks if this peer is providing
    /// the requested actor name without querying remote peers.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to search for locally
    ///
    /// # Returns
    ///
    /// The actor registration if found locally, `None` if not found,
    /// or an error if the lookup fails.
    pub fn lookup_local(
        &mut self,
        name: &str,
    ) -> Result<Option<ActorRegistration<'static>>, RegistryError> {
        // Check if we're providing this key locally
        let key = kad::RecordKey::new(&name);
        let store_mut = self.kademlia.store_mut();
        let is_providing = store_mut.provided().any(|k| k.key == key);

        if is_providing {
            // Get metadata for local provider
            let metadata_key = format!("{name}:meta:{}", self.local_peer_id);
            store_mut
                .get(&kad::RecordKey::new(&metadata_key))
                .map(|record| {
                    ActorRegistration::from_bytes(&record.value)
                        .map(ActorRegistration::into_owned)
                        .map_err(RegistryError::from)
                })
                .transpose()
        } else {
            Ok(None)
        }
    }

    /// Cancels an ongoing actor lookup.
    ///
    /// This is a low-level method. Returns `true` if the lookup was found
    /// and cancelled, `false` if no lookup with the given query ID exists.
    pub fn cancel_lookup(&mut self, query_id: &kad::QueryId) -> bool {
        self.lookup_queries.remove(query_id).is_some()
    }

    pub(super) fn register_with_reply(
        &mut self,
        name: Arc<str>,
        registration: ActorRegistration<'static>,
        reply: Option<RegisterReply>,
    ) -> Result<kad::QueryId, (Option<RegisterReply>, kad::store::Error)> {
        let key = kad::RecordKey::new(&name.as_bytes());
        let provider_query_id = match self.kademlia.start_providing(key) {
            Ok(id) => id,
            Err(err) => {
                return Err((reply, err));
            }
        };

        self.registration_queries.insert(
            provider_query_id,
            RegistrationQuery {
                name,
                registration: Some(registration),
                put_query_id: None,
                provider_result: None,
                reply,
            },
        );

        Ok(provider_query_id)
    }

    pub(super) fn lookup_with_reply(
        &mut self,
        name: Arc<str>,
        reply: Option<LookupReply>,
    ) -> kad::QueryId {
        let query_id = self
            .kademlia
            .get_providers(kad::RecordKey::new(&name.as_bytes()));
        self.lookup_queries.insert(
            query_id,
            LookupQuery {
                name,
                providers_finished: false,
                metadata_queries: HashSet::new(),
                queried_providers: HashSet::new(),
                reported_providers: HashSet::new(),
                reply,
            },
        );

        query_id
    }


}
