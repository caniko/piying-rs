impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = THandler<kad::Behaviour<kad::store::MemoryStore>>;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
        port_use: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        // We need to manually add the address, because kademlia doesn't do this by default (yet)
        // https://github.com/libp2p/rust-libp2p/issues/5313
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                failed_addresses,
                ..
            }) => {
                self.pending_peers.retain(|(pending_peer_id, addr), _| {
                    // Keep all entries for different peers
                    pending_peer_id != &peer_id
                    // OR keep same-peer entries that failed
                    || failed_addresses.iter().any(|failed_addr| failed_addr == addr)
                });
            }
            FromSwarm::NewExternalAddrOfPeer(NewExternalAddrOfPeer { peer_id, addr }) => {
                self.kademlia.add_address(&peer_id, addr.clone());
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error: DialError::Transport(errors),
                ..
            }) => {
                let now = Instant::now();
                for (addr, _) in errors {
                    self.pending_peers
                        .entry((peer_id, addr.clone()))
                        .or_insert(now);
                }
            }
            _ => {}
        }

        self.kademlia.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.kademlia
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        loop {
            // First priority: return any pending events
            if let Some(ev) = self.pending_events.pop_front() {
                // Wake if we have more pending events to process
                if !self.pending_events.is_empty() {
                    cx.waker().wake_by_ref();
                }
                return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
            }

            // Second priority: poll Kademlia for new events
            match self.kademlia.poll(cx) {
                task::Poll::Ready(ToSwarm::GenerateEvent(ev)) => {
                    let (wake, ev) = self.handle_kademlia_event(ev);

                    // If we have an immediate event to return, return it
                    if let Some(ev) = ev {
                        // Wake if requested by handler OR if we have pending events
                        if wake || !self.pending_events.is_empty() {
                            cx.waker().wake_by_ref();
                        }
                        return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
                    }

                    // No immediate event, but if wake was requested or we have pending events,
                    // continue the loop to process them
                    if wake || !self.pending_events.is_empty() {
                        continue;
                    }

                    // No immediate event and no wake needed, continue polling Kademlia
                    // in case it has more events queued
                    continue;
                }
                task::Poll::Ready(other_ev) => {
                    // Non-GenerateEvent from Kademlia (dial events, etc.)
                    if !self.pending_events.is_empty() {
                        cx.waker().wake_by_ref();
                    }

                    return task::Poll::Ready(
                        other_ev.map_out(|_| unreachable!("we handled GenerateEvent above")),
                    );
                }
                task::Poll::Pending => {
                    // Kademlia has no more work ready
                    // Final check: do we have any pending events that were added by the last handler call?
                    if !self.pending_events.is_empty() {
                        continue; // Go back to the top to process them
                    }

                    // Nothing left to do
                    return task::Poll::Pending;
                }
            }
        }
    }
}

