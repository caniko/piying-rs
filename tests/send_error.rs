use piying::error::SendError;

#[test]
fn boxed_send_errors_round_trip_every_variant_and_payload() {
    let errors = [
        SendError::<String, u32>::ActorNotRunning("not running".into()),
        SendError::ActorStopped,
        SendError::MailboxFull("full".into()),
        SendError::HandlerError(9),
        SendError::Timeout(Some("timeout".into())),
        SendError::Timeout(None),
    ];
    for error in errors {
        assert_eq!(
            error.clone().boxed().try_downcast::<String, u32>().unwrap(),
            error
        );
    }
}
