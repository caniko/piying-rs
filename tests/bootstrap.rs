#![cfg(all(
    feature = "remote-tcp",
    feature = "serde-codec",
    not(feature = "rkyv-codec")
))]

// One bootstrap per test binary: it installs the process-wide actor swarm.
#[tokio::test]
async fn bootstrap_listens_on_enabled_transport() {
    #[cfg(feature = "remote-quic")]
    let addr = "/ip4/127.0.0.1/udp/0/quic-v1";
    #[cfg(not(feature = "remote-quic"))]
    let addr = "/ip4/127.0.0.1/tcp/0";

    piying::remote::bootstrap_on(addr).expect("bootstrap swarm should listen");
}
