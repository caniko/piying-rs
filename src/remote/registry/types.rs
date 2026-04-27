struct RegistrationQuery {
    name: Arc<str>,
    registration: Option<ActorRegistration<'static>>,
    put_query_id: Option<kad::QueryId>,
    provider_result: Option<kad::AddProviderResult>,
    reply: Option<RegisterReply>,
}

struct LookupQuery {
    name: Arc<str>,
    providers_finished: bool,
    metadata_queries: HashSet<kad::QueryId>,
    queried_providers: HashSet<PeerId>,
    reported_providers: HashSet<PeerId>,
    reply: Option<LookupReply>,
}

impl LookupQuery {
    fn get_metadata_record(
        &mut self,
        kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
        provider: &PeerId,
    ) -> bool {
        // Skip if we've already queried this provider
        if self.queried_providers.contains(provider) {
            return false;
        }

        self.queried_providers.insert(*provider);
        let key = format!("{}:meta:{provider}", self.name);
        let query_id = kademlia.get_record(key.into_bytes().into());
        self.metadata_queries.insert(query_id);
        true
    }

    fn has_metadata_query(&self, query_id: &kad::QueryId) -> bool {
        self.metadata_queries.contains(query_id)
    }

    fn metadata_query_finished(&mut self, query_id: &kad::QueryId) {
        self.metadata_queries.remove(query_id);
    }

    fn is_finished(&self) -> bool {
        self.providers_finished && self.metadata_queries.is_empty()
    }
}

/// Represents an actor registration in the distributed registry.
///
/// Contains the actor's unique ID and its remote type identifier,
/// which together allow remote peers to locate and communicate
/// with the actor.
#[derive(Debug)]
pub struct ActorRegistration<'a> {
    /// The unique identifier of the actor.
    pub actor_id: ActorId,
    /// The remote type identifier for the actor.
    pub remote_id: Cow<'a, str>,
}

impl<'a> ActorRegistration<'a> {
    /// Creates a new actor registration.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The unique identifier of the actor
    /// * `remote_id` - The remote type identifier for the actor
    pub fn new(actor_id: ActorId, remote_id: Cow<'a, str>) -> Self {
        ActorRegistration {
            actor_id,
            remote_id,
        }
    }

    /// Serializes the actor registration into bytes for storage in the DHT.
    ///
    /// The format includes the peer ID length, actor ID bytes, and remote ID string.
    pub fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 8 + 42 + self.remote_id.len());
        let actor_id_bytes = self.actor_id.to_bytes();
        let peer_id_len = (actor_id_bytes.len() - 8) as u8;
        bytes.extend_from_slice(&peer_id_len.to_le_bytes());
        bytes.extend_from_slice(&actor_id_bytes);
        bytes.extend_from_slice(self.remote_id.as_bytes());
        bytes
    }

    /// Deserializes an actor registration from bytes retrieved from the DHT.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The serialized registration data
    ///
    /// # Returns
    ///
    /// The deserialized actor registration, or an error if the data is invalid.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, InvalidActorRegistration> {
        if bytes.is_empty() {
            return Err(InvalidActorRegistration::EmptyActorRegistration);
        }

        let peer_id_bytes_len = u8::from_le_bytes(bytes[..1].try_into().unwrap()) as usize;
        let actor_id = ActorId::from_bytes(&bytes[1..1 + 8 + peer_id_bytes_len])?;
        let remote_id = std::str::from_utf8(&bytes[1 + 8 + peer_id_bytes_len..])
            .map_err(InvalidActorRegistration::InvalidRemoteIDUtf8)?;

        Ok(ActorRegistration::new(actor_id, Cow::Borrowed(remote_id)))
    }

    /// Converts a borrowed actor registration into an owned one.
    ///
    /// This is useful when you need to store the registration beyond
    /// the lifetime of the original borrowed data.
    pub fn into_owned(self) -> ActorRegistration<'static> {
        ActorRegistration::new(self.actor_id, Cow::Owned(self.remote_id.into_owned()))
    }
}

/// Errors that can occur when deserializing an actor registration.
#[derive(Debug)]
pub enum InvalidActorRegistration {
    /// The registration data is empty.
    EmptyActorRegistration,
    /// Failed to parse the actor ID from bytes.
    ActorId(ActorIdFromBytesError),
    /// The remote ID contains invalid UTF-8.
    InvalidRemoteIDUtf8(str::Utf8Error),
}

impl From<ActorIdFromBytesError> for InvalidActorRegistration {
    fn from(err: ActorIdFromBytesError) -> Self {
        InvalidActorRegistration::ActorId(err)
    }
}

impl fmt::Display for InvalidActorRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidActorRegistration::EmptyActorRegistration => {
                write!(f, "empty actor registration")
            }
            InvalidActorRegistration::ActorId(err) => err.fmt(f),
            InvalidActorRegistration::InvalidRemoteIDUtf8(err) => err.fmt(f),
        }
    }
}
