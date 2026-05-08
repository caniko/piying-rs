//! Session isolation helper for libp2p protocol identifiers.
//!
//! Production builds (`session-isolation` feature off, default) compile to a
//! pass-through that returns the base protocol id unchanged. Test builds
//! (feature on) read `THESPAN_SESSION_ID` once on first access and append it
//! to every protocol id returned, so two co-running test sessions cannot
//! cross-discover via mDNS / KAD even on the same UDP port.
//!
//! Used inside thespis to namespace `/thespis/registry/1.0.0` and
//! `/thespis/messaging/1.0.0`. Downstream crates (e.g. `thespan`) re-export
//! the same helper to namespace their own protocol identifiers and gossipsub
//! topics with the same suffix, so all wire identifiers stay aligned.

/// Apply the active session-isolation suffix to a base protocol id.
///
/// Without the `session-isolation` feature, returns `base.to_owned()`
/// unchanged. With the feature on, appends `/{THESPAN_SESSION_ID}` if the env
/// var is set and non-empty.
///
/// The env var is read once on first call and cached for the lifetime of
/// the process — toggling it after a peer has connected has no effect.
#[inline]
pub fn applied_protocol(base: &str) -> String {
    #[cfg(feature = "session-isolation")]
    {
        if let Some(id) = active_session_id() {
            return format!("{base}/{id}");
        }
    }
    base.to_owned()
}

#[cfg(feature = "session-isolation")]
fn active_session_id() -> Option<&'static str> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<Option<String>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            std::env::var("THESPAN_SESSION_ID")
                .ok()
                .filter(|s| !s.is_empty())
        })
        .as_deref()
}

#[cfg(test)]
mod tests {
    use super::applied_protocol;

    #[cfg(feature = "session-isolation")]
    #[test]
    fn applied_protocol_appends_session_id_when_env_set() {
        // The `OnceLock` cache is keyed on the process; setting the env var
        // before the first call seeds the cache. If another test in this
        // process already seeded a different value, `applied_protocol` still
        // returns *some* suffixed id, which is enough to prove the feature
        // is active.
        unsafe { std::env::set_var("THESPAN_SESSION_ID", "test-isolated-42") };
        let out = applied_protocol("/thespis/messaging/1.0.0");
        assert!(
            out.starts_with("/thespis/messaging/1.0.0/"),
            "expected suffixed protocol id, got {out:?}",
        );
    }

    /// With the feature disabled the helper is a pass-through regardless of
    /// `THESPAN_SESSION_ID` — production builds get the canonical id.
    #[cfg(not(feature = "session-isolation"))]
    #[test]
    fn applied_protocol_is_pass_through_without_feature() {
        unsafe { std::env::set_var("THESPAN_SESSION_ID", "ignored-when-feature-off") };
        let out = applied_protocol("/thespis/messaging/1.0.0");
        assert_eq!(out, "/thespis/messaging/1.0.0");
    }
}
