impl Message<ExchangeDeclare> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: ExchangeDeclare,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.exchange.is_empty() || self.exchanges.contains_key(&msg.exchange) {
            return Err(AmqpError::ExchangeAlreadyExists);
        }

        self.exchanges.insert(
            msg.exchange.clone(),
            Exchange {
                name: msg.exchange,
                kind: msg.kind,
                auto_delete: msg.auto_delete,
                bindings: Vec::new(),
            },
        );
        Ok(())
    }
}

impl Message<ExchangeDelete> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: ExchangeDelete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.exchanges.get(&msg.exchange) {
            Some(exchange) => {
                if msg.if_unused && !exchange.bindings.is_empty() {
                    return Err(AmqpError::ExchangeInUse);
                } else {
                    self.exchanges.remove(&msg.exchange);
                }
            }
            None => {
                return Err(AmqpError::ExchangeNotFound);
            }
        }

        Ok(())
    }
}

impl Message<QueueDeclare> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueDeclare,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueAlreadyExists);
        }

        self.queues.insert(
            msg.queue.clone(),
            Queue {
                auto_delete: msg.auto_delete,
                recipients: HashMap::new(),
            },
        );

        self.default_exchange.bindings.push(Binding {
            queue_name: msg.queue.clone(),
            routing_key: msg.queue.clone(),
            header_match: None,
        });
        Ok(())
    }
}

impl Message<QueueDelete> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueDelete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.queue_delete(msg.queue.clone(), msg.if_unused)?;
        Ok(())
    }
}

impl Message<QueueBind> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueBind,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self.queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueNotFound);
        }

        let exchange = self
            .exchanges
            .get_mut(&msg.exchange)
            .ok_or(AmqpError::ExchangeNotFound)?;

        if exchange
            .bindings
            .iter()
            .any(|b| b.queue_name == msg.queue && b.routing_key == msg.routing_key)
        {
            return Err(AmqpError::BindingAlreadyExists);
        }

        let header_match = if exchange.kind == ExchangeType::Headers {
            let x_match = msg
                .arguments
                .get("x-match")
                .map(|s| s.as_str())
                .unwrap_or("all");

            let mut match_args = msg.arguments.clone();
            match_args.retain(|key, _| !key.starts_with("x-"));

            match x_match {
                "all" => Some(HeaderMatch::All(match_args)),
                "any" => Some(HeaderMatch::Any(match_args)),
                _ => return Err(AmqpError::InvalidHeaderMatch),
            }
        } else {
            None
        };

        exchange.bindings.push(Binding {
            queue_name: msg.queue,
            routing_key: msg.routing_key,
            header_match,
        });
        Ok(())
    }
}

impl Message<QueueUnbind> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueUnbind,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut entry = match self.exchanges.entry(msg.exchange.clone()) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => return Err(AmqpError::ExchangeNotFound),
        };

        let exchange = entry.get_mut();
        exchange
            .bindings
            .retain(|b| !(b.queue_name == msg.queue && b.routing_key == msg.routing_key));

        if exchange.bindings.is_empty() && exchange.auto_delete {
            entry.remove();
        }
        Ok(())
    }
}

impl<M> Message<BasicPublish<M>> for MessageQueue
where
    M: Clone + Send + Sync + 'static,
{
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicPublish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let exchange = if msg.exchange.is_empty() {
            &self.default_exchange
        } else if let Some(exchange) = self.exchanges.get(&msg.exchange) {
            exchange
        } else {
            return Err(AmqpError::ExchangeNotFound);
        };

        let filter = msg.properties.filter.unwrap_or(|_| true);
        let mut target_queues = HashSet::new();

        match exchange.kind {
            ExchangeType::Direct => {
                for binding in &exchange.bindings {
                    if binding.routing_key == msg.routing_key {
                        target_queues.insert(binding.queue_name.clone());
                    }
                }
            }
            ExchangeType::Topic => {
                let options = MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                };

                for binding in &exchange.bindings {
                    if Pattern::new(&binding.routing_key)
                        .unwrap()
                        .matches_with(&msg.routing_key, options)
                    {
                        target_queues.insert(binding.queue_name.clone());
                    }
                }
            }
            ExchangeType::Fanout => {
                for binding in &exchange.bindings {
                    target_queues.insert(binding.queue_name.clone());
                }
            }
            ExchangeType::Headers => {
                let message_headers = msg
                    .properties
                    .headers
                    .as_ref()
                    .ok_or(AmqpError::HeadersRequired)?;

                for binding in &exchange.bindings {
                    if let Some(header_match) = &binding.header_match
                        && header_match.matches(message_headers)
                    {
                        target_queues.insert(binding.queue_name.clone());
                    }
                }
            }
        }

        for queue_name in target_queues {
            self.delivery_message(queue_name, &msg.message, ctx.actor_ref(), filter)
                .await
        }

        Ok(())
    }
}

impl<M: Send + 'static> Message<BasicConsume<M>> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicConsume<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let queue = self
            .queues
            .get_mut(&msg.queue)
            .ok_or(AmqpError::QueueNotFound)?;
        let actor_id = msg.recipient.id();
        let recipients = queue.recipients.entry(TypeId::of::<M>()).or_default();

        if !recipients.iter().any(|reg| reg.actor_id == actor_id) {
            recipients.push(Registration {
                actor_id,
                recipient: Box::new(msg.recipient),
                tags: msg.tags,
            });
        }
        Ok(())
    }
}

impl<M: Send + 'static> Message<BasicCancel<M>> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicCancel<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.basic_cancel(msg.queue, msg.recipient)
    }
}
