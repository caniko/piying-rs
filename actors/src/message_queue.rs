//! Provides an AMQP-style message queue system for the actor system.
//!
//! The `message_queue` module implements a flexible message queue system inspired by AMQP, with
//! support for different exchange types (direct, topic, fanout, headers) and queue semantics.
//! It allows actors to communicate through named queues and exchanges with various routing rules.
//!
//! # Features
//!
//! - **Multiple Exchange Types**: Supports direct, topic, fanout, and header-based routing.
//! - **Flexible Routing**: Messages can be routed based on routing keys or header matching.
//! - **Queue Management**: Supports queue declaration, binding, and automatic cleanup.
//! - **Multiple Delivery Strategies**: Configure how messages are delivered to handle different reliability needs.
//! - **Automatic Cleanup**: Dead actor references are automatically removed from consumer lists.
//!
//! # Example
//!
//! ```
//! use std::collections::HashMap;
//!
//! use thespis::prelude::*;
//! use thespis_actors::DeliveryStrategy;
//! use thespis_actors::message_queue::{
//!     BasicConsume, BasicPublish, ExchangeDeclare, ExchangeType, MessageQueue, QueueBind,
//!     QueueDeclare,
//! };
//!
//! #[derive(Clone)]
//! struct OrderEvent {
//!     product_id: String,
//!     quantity: u32,
//! }
//!
//! #[derive(Actor)]
//! struct OrderProcessor;
//!
//! # impl Message<OrderEvent> for OrderProcessor {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: OrderEvent, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Create a message queue with best effort delivery
//! let mq = MessageQueue::new(DeliveryStrategy::BestEffort);
//! let mq_ref = MessageQueue::spawn(mq);
//!
//! // Set up exchange and queue
//! mq_ref.tell(ExchangeDeclare {
//!     exchange: "orders".to_string(),
//!     kind: ExchangeType::Topic,
//!     auto_delete: false,
//! }).await?;
//! mq_ref.tell(QueueDeclare {
//!     queue: "order_processing".to_string(),
//!     auto_delete: false,
//! }).await?;
//! mq_ref.tell(QueueBind {
//!     queue: "order_processing".to_string(),
//!     exchange: "orders".to_string(),
//!     routing_key: "order.*".to_string(),
//!     arguments: Default::default(),
//! }).await?;
//!
//! // Register a consumer
//! let processor = OrderProcessor::spawn(OrderProcessor);
//! mq_ref.tell(BasicConsume {
//!     queue: "order_processing".to_string(),
//!     recipient: processor.recipient::<OrderEvent>(),
//!     tags: Default::default(),
//! }).await?;
//!
//! // Publish an order event
//! mq_ref.tell(BasicPublish {
//!     exchange: "orders".to_string(),
//!     routing_key: "order.created".to_string(),
//!     message: OrderEvent {
//!         product_id: "123".to_string(),
//!         quantity: 2,
//!     },
//!     properties: Default::default(),
//! }).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet, hash_map::Entry},
};

use glob::{MatchOptions, Pattern};
use thespis::prelude::*;

use crate::DeliveryStrategy;

pub type FilterFn = fn(&HashMap<String, String>) -> bool;

/// The main message queue actor that manages exchanges, queues and message routing.
///
/// This actor implements AMQP-style messaging semantics with support for:
/// - Multiple exchange types (direct, topic, fanout, headers)
/// - Queue declarations and bindings
/// - Message publishing and consumption
/// - Automatic cleanup of unused resources
///
/// Messages are delivered according to the configured delivery strategy, allowing
/// for different reliability and performance trade-offs.
#[derive(Actor, Debug)]
pub struct MessageQueue {
    /// Registered exchanges by name
    exchanges: HashMap<String, Exchange>,
    /// Registered queues by name
    queues: HashMap<String, Queue>,
    /// The default direct exchange
    default_exchange: Exchange,
    /// Delivery strategy for messages
    delivery_strategy: DeliveryStrategy,
}

/// Properties associated with a published message
#[derive(Debug, Clone, Default)]
pub struct MessageProperties {
    /// Optional headers for header-based routing
    pub headers: Option<HashMap<String, String>>,
    /// Optional filter function for message routing based on consumer tags
    /// The function takes a reference to the consumer's tags and returns true if the message should be delivered
    pub filter: Option<FilterFn>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExchangeType {
    /// Direct exchange - routes based on exact routing key match
    #[default]
    Direct,
    /// Topic exchange - routes based on pattern matching
    Topic,
    /// Fanout exchange - routes to all bound queues
    Fanout,
    /// Headers exchange - routes based on header matching
    Headers,
}

/// Internal enum for header matching rules
#[derive(Debug, Clone)]
enum HeaderMatch {
    /// All specified headers must match
    All(HashMap<String, String>),
    /// Any specified header must match
    Any(HashMap<String, String>),
}

impl HeaderMatch {
    /// Checks if the given headers match the rules
    pub fn matches(&self, headers: &HashMap<String, String>) -> bool {
        match self {
            HeaderMatch::All(rules) => rules
                .iter()
                .all(|(k, v)| headers.get(k).is_some_and(|val| val == v)),
            HeaderMatch::Any(rules) => rules
                .iter()
                .any(|(k, v)| headers.get(k).is_some_and(|val| val == v)),
        }
    }
}

/// Represents an exchange in the message queue
#[derive(Debug)]
struct Exchange {
    /// Exchange name
    name: String,
    /// Exchange type
    kind: ExchangeType,
    /// Whether to auto-delete when unused
    auto_delete: bool,
    /// List of queue bindings
    bindings: Vec<Binding>,
}

/// Represents a binding between an exchange and queue
#[derive(Debug)]
struct Binding {
    /// Name of bound queue
    queue_name: String,
    /// Routing key or pattern
    routing_key: String,
    /// Optional header matching rules
    header_match: Option<HeaderMatch>,
}

/// Represents a message queue
#[derive(Debug)]
struct Queue {
    /// Whether to auto-delete when unused
    auto_delete: bool,
    /// Registered consumers by message type
    recipients: HashMap<TypeId, Vec<Registration>>,
}

/// Represents a registered message consumer
#[derive(Debug)]
struct Registration {
    /// ID of consuming actor
    actor_id: ActorId,
    /// Typed recipient for messages
    recipient: Box<dyn Any + Send + Sync>,
    /// Key-value tags associated with this consumer that can be used for filtered message delivery
    /// These tags are checked against the message's filter function (if provided) to determine
    /// whether this consumer should receive the message
    tags: HashMap<String, String>,
}

/// Error types for message queue operations
#[derive(Debug, thiserror::Error)]
pub enum AmqpError {
    #[error("Exchange already exists")]
    ExchangeAlreadyExists,
    #[error("Queue already exists")]
    QueueAlreadyExists,
    #[error("Exchange not found")]
    ExchangeNotFound,
    #[error("Queue not found")]
    QueueNotFound,
    #[error("Binding already exists")]
    BindingAlreadyExists,
    #[error("Headers required")]
    HeadersRequired,
    #[error("Invalid header match")]
    InvalidHeaderMatch,
    #[error("Exchange in use")]
    ExchangeInUse,
    #[error("Queue in use")]
    QueueInUse,
}

/// Message for declaring a new exchange
#[derive(Debug, Default)]
pub struct ExchangeDeclare {
    /// Exchange name
    pub exchange: String,
    /// Exchange type
    pub kind: ExchangeType,
    /// Whether to auto-delete when unused
    pub auto_delete: bool,
}

/// Message for deleting an exchange
#[derive(Debug, Default)]
pub struct ExchangeDelete {
    /// Exchange name
    pub exchange: String,
    /// Only delete if unused
    pub if_unused: bool,
}

/// Message for declaring a new queue
#[derive(Debug, Default)]
pub struct QueueDeclare {
    /// Queue name
    pub queue: String,
    /// Whether to auto-delete when unused
    pub auto_delete: bool,
}

/// Message for deleting a queue
#[derive(Debug, Default)]
pub struct QueueDelete {
    /// Queue name
    pub queue: String,
    /// Only delete if unused
    pub if_unused: bool,
}

/// Message for binding a queue to an exchange
#[derive(Debug, Default)]
pub struct QueueBind {
    /// Queue name
    pub queue: String,
    /// Exchange name
    pub exchange: String,
    /// Routing key/pattern
    pub routing_key: String,
    /// Additional binding arguments
    pub arguments: HashMap<String, String>,
}

/// Message for unbinding a queue from an exchange
#[derive(Debug, Default)]
pub struct QueueUnbind {
    /// Queue name
    pub queue: String,
    /// Exchange name
    pub exchange: String,
    /// Routing key/pattern
    pub routing_key: String,
}

/// Message for publishing a message to an exchange
#[derive(Debug)]
pub struct BasicPublish<M>
where
    M: Clone + Send + 'static,
{
    /// Exchange name (empty for default)
    pub exchange: String,
    /// Routing key
    pub routing_key: String,
    /// Message payload
    pub message: M,
    /// Message properties
    pub properties: MessageProperties,
}

/// Message for consuming messages from a queue
#[derive(Debug)]
pub struct BasicConsume<M: Send + 'static> {
    /// Queue name
    pub queue: String,
    /// Recipient for messages
    pub recipient: Recipient<M>,
    /// Tags associated with this consumer that can be used for filtered message delivery
    /// These tags will be passed to the message's filter function (if provided) when determining
    /// whether this consumer should receive published messages
    pub tags: HashMap<String, String>,
}

/// Message for canceling a consumer
#[derive(Debug)]
pub struct BasicCancel<M: Send + 'static> {
    /// Queue name
    pub queue: String,
    /// Recipient to cancel
    pub recipient: Recipient<M>,
}

include!("message_queue/core_impl.rs");
include!("message_queue/handlers.rs");
