impl MessageQueue {
    /// Creates a new message queue with the specified delivery strategy.
    ///
    /// # Arguments
    ///
    /// * `delivery_strategy` - Determines how messages are delivered to consumers
    ///
    /// # Returns
    ///
    /// A new `MessageQueue` instance with the specified delivery strategy
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        Self {
            exchanges: HashMap::new(),
            queues: HashMap::new(),
            default_exchange: Exchange {
                name: "".to_string(),
                kind: ExchangeType::Direct,
                auto_delete: false,
                bindings: Vec::new(),
            },
            delivery_strategy,
        }
    }

    fn queue_delete(&mut self, queue_name: String, if_unused: bool) -> Result<(), AmqpError> {
        match self.queues.get(&queue_name) {
            Some(queue) => {
                if if_unused && !queue.recipients.is_empty() {
                    return Err(AmqpError::QueueInUse);
                }
                self.queues.remove(&queue_name);
            }
            None => {
                return Err(AmqpError::QueueNotFound);
            }
        }

        self.default_exchange
            .bindings
            .retain(|b| b.queue_name != queue_name);

        let mut exchanges_to_delete = Vec::new();
        for exchange in self.exchanges.values_mut() {
            exchange.bindings.retain(|b| b.queue_name != queue_name);
            if exchange.bindings.is_empty() && exchange.auto_delete {
                exchanges_to_delete.push(exchange.name.clone());
            }
        }
        for exchange_name in exchanges_to_delete {
            self.exchanges.remove(&exchange_name);
        }
        Ok(())
    }

    fn basic_cancel<M: Send + 'static>(
        &mut self,
        queue_name: String,
        recipient: Recipient<M>,
    ) -> Result<(), AmqpError> {
        let queue_delete = {
            let queue = self
                .queues
                .get_mut(&queue_name)
                .ok_or(AmqpError::QueueNotFound)?;
            let type_id = TypeId::of::<M>();
            if let Some(recipients) = queue.recipients.get_mut(&type_id) {
                recipients.retain(|registration| registration.actor_id != recipient.id());
            }
            queue.recipients.retain(|_, v| !v.is_empty());

            queue.auto_delete
        };

        if queue_delete {
            self.queue_delete(queue_name, true)?;
        }

        Ok(())
    }

    async fn delivery_message<M>(
        &mut self,
        queue_name: String,
        message: &M,
        self_ref: &ActorRef<Self>,
        filter: impl Fn(&HashMap<String, String>) -> bool,
    ) where
        M: Clone + Send + 'static,
    {
        let mut to_cancel = Vec::new();
        if let Some(recipients) = self
            .queues
            .get(&queue_name)
            .and_then(|queue| queue.recipients.get(&TypeId::of::<M>()))
        {
            for regis in recipients.iter().filter(|regis| filter(&regis.tags)) {
                let queue_name = queue_name.clone();
                let recipient: &Recipient<M> = regis.recipient.downcast_ref().unwrap();
                match self.delivery_strategy {
                    DeliveryStrategy::Guaranteed => {
                        let res = recipient.tell(message.clone()).await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::BestEffort => {
                        let res = recipient.tell(message.clone()).try_send();
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::TimedDelivery(duration) => {
                        let res = recipient
                            .tell(message.clone())
                            .mailbox_timeout(duration)
                            .await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::Spawned => {
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let self_ref = self_ref.clone();
                        tokio::spawn(async move {
                            let res = recipient.tell(message).send().await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = self_ref
                                    .tell(BasicCancel::<M> {
                                        queue: queue_name.clone(),
                                        recipient: recipient.clone(),
                                    })
                                    .await;
                            }
                        });
                    }
                    DeliveryStrategy::SpawnedWithTimeout(duration) => {
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let self_ref = self_ref.clone();
                        tokio::spawn(async move {
                            let res = recipient
                                .tell(message)
                                .mailbox_timeout(duration)
                                .send()
                                .await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = self_ref
                                    .tell(BasicCancel::<M> {
                                        queue: queue_name.clone(),
                                        recipient: recipient.clone(),
                                    })
                                    .await;
                            }
                        });
                    }
                }
            }
        }

        for recipient in to_cancel {
            let _ = self.basic_cancel::<M>(queue_name.clone(), recipient);
        }
    }
}

