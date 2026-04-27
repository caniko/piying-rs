impl Behaviour {
    fn handle_kademlia_event(&mut self, ev: kad::Event) -> (bool, Option<Event>) {
        match ev {
            kad::Event::InboundRequest { request } => {
                match request {
                    kad::InboundRequest::AddProvider { record } => {
                        let record =
                            record.expect("filtering is enabled, so the record should be present");

                        if self.validate_provider_registration(&record) {
                            // Accept the provider registration
                            #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                            if let Err(err) = self.kademlia.store_mut().add_provider(record) {
                                #[cfg(feature = "tracing")]
                                tracing::warn!("failed to store provider: {err}");
                            }
                        }

                        (false, None)
                    }
                    kad::InboundRequest::PutRecord { source, record, .. } => {
                        let record =
                            record.expect("filtering is enabled, so the record should be present");

                        if self.validate_metadata_record(&source, &record) {
                            // Store the metadata record
                            #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                            if let Err(err) = self.kademlia.store_mut().put(record) {
                                #[cfg(feature = "tracing")]
                                tracing::warn!("failed to store metadata record: {err}");
                            }
                        }

                        (false, None)
                    }
                    _ => (false, None),
                }
            }
            kad::Event::OutboundQueryProgressed {
                id,
                result,
                stats: _,
                step,
            } => match result {
                #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                kad::QueryResult::Bootstrap(res) => {
                    #[cfg(feature = "tracing")]
                    if let Err(err) = res {
                        tracing::warn!("bootstrap failed: {err}");
                    }

                    let failed_peers = self
                        .pending_peers
                        .extract_if(|_, failed_at| failed_at.elapsed() > Duration::from_secs(5));
                    for ((peer_id, addr), _) in failed_peers {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(%peer_id, %addr, "removing address for peer");
                        self.kademlia.remove_address(&peer_id, &addr);
                    }

                    (false, None)
                }
                kad::QueryResult::GetClosestPeers(_) => (false, None),
                // Getting the providers has progressed
                kad::QueryResult::GetProviders(res) => {
                    let Entry::Occupied(mut lookup_query_entry) = self.lookup_queries.entry(id)
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring GetProviders event for unknown lookup query");
                        return (false, None);
                    };
                    let lookup_query = lookup_query_entry.get_mut();

                    match res {
                        Ok(kad::GetProvidersOk::FoundProviders { providers, .. }) => {
                            let mut wake = false;
                            lookup_query.providers_finished = step.last;

                            for provider in providers {
                                wake |=
                                    lookup_query.get_metadata_record(&mut self.kademlia, &provider);
                            }

                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (wake, None)
                        }
                        Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. }) => {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (false, None)
                        }
                        Err(kad::GetProvidersError::Timeout { .. }) => {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            match &lookup_query.reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::Timeout));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupTimeout {
                                        provider_query_id: id,
                                    });
                                }
                            }
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (false, None)
                        }
                    }
                }
                // Registering an actor has progressed
                kad::QueryResult::StartProviding(res) => {
                    let Entry::Occupied(mut registration_query_entry) =
                        self.registration_queries.entry(id)
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "ignoring StartProviding event for unknown registration query"
                        );
                        return (false, None);
                    };
                    let registration_query = registration_query_entry.get_mut();

                    match res {
                        Ok(kad::AddProviderOk { .. }) => {
                            let Some(registration) = registration_query.registration.take() else {
                                panic!("the registration should exist here");
                            };
                            registration_query.provider_result = Some(res.clone());

                            // Store the metadata record
                            let key =
                                format!("{}:meta:{}", registration_query.name, self.local_peer_id);
                            let registration_bytes = registration.into_bytes();
                            let record = kad::Record::new(key.into_bytes(), registration_bytes);

                            match self.kademlia.put_record(record, kad::Quorum::One) {
                                Ok(put_query_id) => {
                                    registration_query.put_query_id = Some(put_query_id);
                                }
                                Err(err) => {
                                    // Put record failed immediately
                                    match registration_query_entry.remove().reply {
                                        Some(tx) => {
                                            let _ = tx.send(Err(err.into()));
                                        }
                                        None => {
                                            self.pending_events.push_back(
                                                Event::RegistrationFailed {
                                                    provider_query_id: id,
                                                    error: err.into(),
                                                },
                                            );
                                        }
                                    }
                                }
                            }

                            (true, None)
                        }
                        Err(kad::AddProviderError::Timeout { .. }) => {
                            match registration_query_entry.remove().reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::Timeout));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegistrationFailed {
                                        provider_query_id: id,
                                        error: RegistryError::Timeout,
                                    });
                                }
                            }

                            (false, None)
                        }
                    }
                }
                kad::QueryResult::RepublishProvider(_) => (false, None),
                // Getting a metadata record has progressed
                kad::QueryResult::GetRecord(res) => {
                    let Some((provider_query_id, lookup_query)) = self
                        .lookup_queries
                        .iter_mut()
                        .find(|(_, lookup_query)| lookup_query.has_metadata_query(&id))
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring GetRecord event for unknown lookup query");
                        return (false, None);
                    };
                    let provider_query_id = *provider_query_id;

                    match res {
                        Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                            record: kad::Record { value, .. },
                            ..
                        })) => {
                            let result = ActorRegistration::from_bytes(&value)
                                .map(|registration| registration.into_owned())
                                .map_err(RegistryError::from);

                            // Check if we've already reported this provider to avoid duplicates
                            let should_emit = if let Ok(ref registration) = result {
                                if let Some(peer_id) = registration.actor_id.peer_id() {
                                    if lookup_query.reported_providers.contains(&peer_id) {
                                        false // Already reported this provider
                                    } else {
                                        lookup_query.reported_providers.insert(peer_id);
                                        true
                                    }
                                } else {
                                    true // No peer_id, emit anyway
                                }
                            } else {
                                true // Error case, emit anyway
                            };

                            if should_emit {
                                match &lookup_query.reply {
                                    Some(tx) => {
                                        let _ = tx.send(result);
                                    }
                                    None => {
                                        self.pending_events.push_back(Event::LookupProgressed {
                                            provider_query_id,
                                            get_query_id: id,
                                            result,
                                        });
                                    }
                                }
                            }
                        }
                        // These cases don't provide useful information to the user
                        Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. })
                        | Err(kad::GetRecordError::NotFound { .. }) => {
                            // No progress event needed
                        }
                        // Error cases are still useful to report
                        Err(kad::GetRecordError::QuorumFailed { quorum, .. }) => {
                            match &lookup_query.reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::QuorumFailed { quorum }));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupProgressed {
                                        provider_query_id,
                                        get_query_id: id,
                                        result: Err(RegistryError::QuorumFailed { quorum }),
                                    });
                                }
                            }
                        }
                        Err(kad::GetRecordError::Timeout { .. }) => match &lookup_query.reply {
                            Some(tx) => {
                                let _ = tx.send(Err(RegistryError::Timeout));
                            }
                            None => {
                                self.pending_events.push_back(Event::LookupProgressed {
                                    provider_query_id,
                                    get_query_id: id,
                                    result: Err(RegistryError::Timeout),
                                });
                            }
                        },
                    }

                    if step.last {
                        lookup_query.metadata_query_finished(&id);
                        let last = lookup_query.is_finished();

                        if last {
                            if lookup_query.reply.is_none() {
                                self.pending_events
                                    .push_back(Event::LookupCompleted { provider_query_id });
                            }
                            self.lookup_queries.remove(&provider_query_id);
                        }
                    }

                    (false, None)
                }
                // Putting a metadata record has progressed
                kad::QueryResult::PutRecord(res) => {
                    let Some(provider_query_id) =
                        self.registration_queries
                            .iter()
                            .find_map(|(query_id, reg)| {
                                if reg.put_query_id == Some(id) {
                                    Some(*query_id)
                                } else {
                                    None
                                }
                            })
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring PutRecord event for unknown registration query");
                        return (false, None);
                    };

                    match res {
                        Ok(ok) => {
                            let mut registration_query = self
                                .registration_queries
                                .remove(&provider_query_id)
                                .unwrap();
                            match registration_query.reply.take() {
                                Some(tx) => {
                                    let _ = tx.send(Ok(()));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegisteredActor {
                                        provider_result: registration_query
                                            .provider_result
                                            .unwrap(),
                                        provider_query_id,
                                        metadata_result: Ok(ok.clone()),
                                        metadata_query_id: id,
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            match self
                                .registration_queries
                                .remove(&provider_query_id)
                                .and_then(|q| q.reply)
                            {
                                Some(tx) => {
                                    let _ = tx.send(Err(err.clone().into()));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegistrationFailed {
                                        provider_query_id,
                                        error: err.clone().into(),
                                    });
                                }
                            }
                        }
                    }

                    (false, None)
                }
                kad::QueryResult::RepublishRecord(_) => (false, None),
            },
            kad::Event::RoutingUpdated {
                peer,
                is_new_peer,
                addresses,
                bucket_range,
                old_peer,
            } => (
                false,
                Some(Event::RoutingUpdated {
                    peer,
                    is_new_peer,
                    addresses,
                    bucket_range,
                    old_peer,
                }),
            ),
            kad::Event::UnroutablePeer { peer } => (false, Some(Event::UnroutablePeer { peer })),
            kad::Event::RoutablePeer { peer, address } => {
                (false, Some(Event::RoutablePeer { peer, address }))
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                (false, Some(Event::PendingRoutablePeer { peer, address }))
            }
            kad::Event::ModeChanged { .. } => (false, None),
        }
    }


}
