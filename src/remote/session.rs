//! Session isolation helper for libp2p protocol identifiers.
//!
//! Production builds (`session-isolation` feature off, default) compile to a
//! pass-through that returns the base protocol id unchanged. Test builds
//! (feature on) read `THESPAN_SESSION_ID` once on first access and append it
//! to every protocol id returned, so two co-running test sessions cannot
//! cross-discover via mDNS / KAD even on the same UDP port.
//!
//! Used inside piying to namespace `/piying/registry/1.0.0` and
//! `/piying/messaging/1.0.0`. Downstream crates (e.g. `thespan`) re-export
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

    /// Base id every case asserts on.
    const BASE: &str = "/piying/messaging/1.0.0";

    /// Session-isolation variable read (once per process) by `applied_protocol`.
    const SESSION_ENV: &str = "THESPAN_SESSION_ID";

    /// Names the case a spawned child must assert.
    const CASE_ENV: &str = "PIYING_SESSION_ID_CASE";

    /// One case, asserted in a freshly started process.
    ///
    /// `applied_protocol` caches the session id on first access for the
    /// lifetime of the process, so a case that sets the variable after another
    /// caller (registry, messaging, a sibling test) has already cached `None`
    /// cannot observe it. Each case therefore gets a process that has never
    /// called it, and the parent never mutates the environment.
    #[test]
    #[ignore = "spawned by session_id_cases_run_in_isolated_processes"]
    fn session_id_case_in_child_process() {
        let case = std::env::var(CASE_ENV).expect("child sets PIYING_SESSION_ID_CASE");
        let isolated = cfg!(feature = "session-isolation");
        let expected = match (case.as_str(), isolated) {
            ("set", true) => format!("{BASE}/test-isolated-42"),
            // The feature off is a pass-through; an empty value is not a
            // session id, so it is ignored too.
            ("set", false) | ("empty", _) | ("unset", _) => BASE.to_owned(),
            (other, _) => panic!("unknown case {other:?}"),
        };
        assert_eq!(
            applied_protocol(BASE),
            expected,
            "case {case:?} with session-isolation={isolated}",
        );
    }

    /// Runs every environment-dependent case in its own process, so the
    /// process-global cache is seeded from the documented once-per-process
    /// read instead of from whatever ran first.
    #[test]
    fn session_id_cases_run_in_isolated_processes() {
        let exe = std::env::current_exe().expect("test binary path");
        for case in ["set", "empty", "unset"] {
            let mut child = std::process::Command::new(&exe);
            child
                .args([
                    "--exact",
                    "--ignored",
                    "remote::session::tests::session_id_case_in_child_process",
                ])
                .env_remove(SESSION_ENV)
                .env(CASE_ENV, case);
            if case == "set" {
                child.env(SESSION_ENV, "test-isolated-42");
            } else if case == "empty" {
                child.env(SESSION_ENV, "");
            }
            let out = child.output().expect("spawn test child process");
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success() && stdout.contains("1 passed"),
                "case {case:?} did not pass in its child process\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&out.stderr),
            );
        }
    }
}
