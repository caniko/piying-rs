/// An error that can occur when registering & looking up actors by name.
#[derive(Debug)]
pub enum RegistryError {
    /// The actor swarm has not been bootstrapped.
    #[cfg(feature = "remote")]
    SwarmNotBootstrapped,
    /// The remote actor was found given the ID, but was not the correct type.
    BadActorType,
    /// An actor has already been registered under the name.
    NameAlreadyRegistered,
    /// Quorum failed.
    #[cfg(feature = "remote-registry")]
    QuorumFailed {
        /// Required quorum.
        quorum: std::num::NonZero<usize>,
    },
    /// Timeout.
    #[cfg(feature = "remote-registry")]
    Timeout,
    /// Storing the record failed.
    #[cfg(feature = "remote-registry")]
    Store(libp2p::kad::store::Error),
    /// Invalid actor registration.
    #[cfg(feature = "remote-registry")]
    InvalidActorRegistration(crate::remote::registry::InvalidActorRegistration),
    /// Get providers error.
    #[cfg(feature = "remote-registry")]
    GetProviders(libp2p::kad::GetProvidersError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "remote")]
            RegistryError::SwarmNotBootstrapped => write!(f, "actor swarm not bootstrapped"),
            RegistryError::NameAlreadyRegistered => write!(f, "name already registered"),
            RegistryError::BadActorType => write!(f, "bad actor type"),
            #[cfg(feature = "remote-registry")]
            RegistryError::QuorumFailed { quorum } => {
                write!(f, "the quorum failed; needed {quorum} peers")
            }
            #[cfg(feature = "remote-registry")]
            RegistryError::Timeout => write!(f, "the request timed out"),
            #[cfg(feature = "remote-registry")]
            RegistryError::Store(err) => err.fmt(f),
            #[cfg(feature = "remote-registry")]
            RegistryError::InvalidActorRegistration(err) => err.fmt(f),
            #[cfg(feature = "remote-registry")]
            RegistryError::GetProviders(err) => err.fmt(f),
        }
    }
}

impl error::Error for RegistryError {}

#[cfg(feature = "remote-registry")]
impl From<crate::remote::registry::InvalidActorRegistration> for RegistryError {
    fn from(err: crate::remote::registry::InvalidActorRegistration) -> Self {
        RegistryError::InvalidActorRegistration(err)
    }
}

#[cfg(feature = "remote-registry")]
impl From<libp2p::kad::store::Error> for RegistryError {
    fn from(err: libp2p::kad::store::Error) -> Self {
        RegistryError::Store(err)
    }
}

#[cfg(feature = "remote-registry")]
impl From<libp2p::kad::AddProviderError> for RegistryError {
    fn from(err: libp2p::kad::AddProviderError) -> Self {
        match err {
            libp2p::kad::AddProviderError::Timeout { .. } => RegistryError::Timeout,
        }
    }
}

#[cfg(feature = "remote-registry")]
impl From<libp2p::kad::PutRecordError> for RegistryError {
    fn from(err: libp2p::kad::PutRecordError) -> Self {
        match err {
            libp2p::kad::PutRecordError::QuorumFailed { quorum, .. } => {
                RegistryError::QuorumFailed { quorum }
            }
            libp2p::kad::PutRecordError::Timeout { .. } => RegistryError::Timeout,
        }
    }
}

#[cfg(feature = "remote-registry")]
impl From<libp2p::kad::GetProvidersError> for RegistryError {
    fn from(err: libp2p::kad::GetProvidersError) -> Self {
        RegistryError::GetProviders(err)
    }
}
