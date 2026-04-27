#[cfg(test)]
mod tests {
    use std::{error, fmt};

    use crate::actor::Spawn;
    use crate::error::Infallible;
    use crate::{
        actor::Actor,
        message::{Context, Message},
    };

    use super::ForwardedReply;

    #[tokio::test]
    async fn test_forwarded_reply_from_ok() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage;

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, String>;

            async fn handle(
                &mut self,
                _msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                // Instead of forwarding, respond directly with a success value
                ForwardedReply::from_ok("Direct response".to_string())
            }
        }

        let actor_ref = TestActor::spawn(TestActor);
        let response = actor_ref.ask(TestMessage).await.unwrap();
        assert_eq!(response, "Direct response");
    }

    #[tokio::test]
    async fn test_forwarded_reply_from_err() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage;

        #[derive(Debug, Clone, PartialEq)]
        struct TestError {
            message: String,
        }

        impl fmt::Display for TestError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.message)
            }
        }

        impl error::Error for TestError {}

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, Result<String, TestError>>;

            async fn handle(
                &mut self,
                _msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                // Instead of forwarding, respond directly with an error
                ForwardedReply::from_err(TestError {
                    message: "Something went wrong".to_string(),
                })
            }
        }

        let actor_ref = TestActor::spawn(TestActor);
        let response = actor_ref.ask(TestMessage).await;

        match response {
            Err(error) => {
                match error.err() {
                    Some(handler_error) => {
                        // handler_error is a SendError<TestMessage, TestError>
                        match handler_error.err() {
                            Some(test_error) => {
                                assert_eq!(test_error.message, "Something went wrong");
                            }
                            None => panic!("Expected inner TestError"),
                        }
                    }
                    None => panic!("Expected handler error"),
                }
            }
            Ok(_) => panic!("Expected error response"),
        }
    }

    #[tokio::test]
    async fn test_forwarded_reply_from_result() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage {
            should_succeed: bool,
        }

        #[derive(Debug, Clone, PartialEq)]
        struct TestError;

        impl fmt::Display for TestError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "Test error")
            }
        }

        impl error::Error for TestError {}

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, Result<i32, TestError>>;

            async fn handle(
                &mut self,
                msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                let result = if msg.should_succeed {
                    Ok(42)
                } else {
                    Err(TestError)
                };

                // Use from_result to create a ForwardedReply from a Result
                ForwardedReply::from_result(result)
            }
        }

        let actor_ref = TestActor::spawn(TestActor);

        // Test success case
        let response = actor_ref
            .ask(TestMessage {
                should_succeed: true,
            })
            .await
            .unwrap();
        assert_eq!(response, 42);

        // Test error case
        let response = actor_ref
            .ask(TestMessage {
                should_succeed: false,
            })
            .await;
        assert!(response.is_err());
    }
}
