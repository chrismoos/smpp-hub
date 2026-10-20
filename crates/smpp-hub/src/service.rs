use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::model::{BindRole, MessageId, Quotas, Site, SiteId};
use crate::store::{NewMessage, Store};
use chrono::Utc;
use smpp_core::{
    Action, Address, CommandStatus, Config, Connection, ConnectionOptions, MessageBody, Pdu,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

use crate::authorize::{Lookup, Rejection, authorize};
use crate::queue::{Outcome, resolve};
use crate::schedule::{RetrySchedule, expiry};
use smpp_core::receipt;

pub const DRAIN_INTERVAL: Duration = Duration::from_secs(15);

const DELIVERY_WINDOW: usize = 16;

#[derive(Default)]
pub struct BoundSites {
    inner: Mutex<HashMap<SiteId, mpsc::Sender<Nudge>>>,
}

pub struct Nudge;

impl BoundSites {
    fn claim(self: &Arc<Self>, site: SiteId) -> Option<BoundSession> {
        let (tx, nudges) = mpsc::channel(1);
        let mut sites = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if sites.contains_key(&site) {
            return None;
        }
        sites.insert(site, tx);
        Some(BoundSession {
            sites: self.clone(),
            site,
            nudges,
        })
    }

    pub async fn is_bound(&self, site: SiteId) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&site)
    }

    pub async fn nudge_all(&self) {
        for tx in self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            let _ = tx.try_send(Nudge);
        }
    }
}

struct BoundSession {
    sites: Arc<BoundSites>,
    site: SiteId,
    nudges: mpsc::Receiver<Nudge>,
}

impl Drop for BoundSession {
    fn drop(&mut self) {
        self.sites
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.site);
    }
}

pub struct Hub {
    pub registry: Arc<dyn Store>,
    pub bound: Arc<BoundSites>,
    pub schedule: RetrySchedule,
    pub system_id: String,
    shutdown: watch::Sender<bool>,
}

impl Hub {
    pub fn new(registry: Arc<dyn Store>, system_id: impl Into<String>) -> Self {
        Self {
            registry,
            bound: Arc::new(BoundSites::default()),
            schedule: RetrySchedule::default(),
            system_id: system_id.into(),
            shutdown: watch::channel(false).0,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub async fn serve_inbound<S>(&self, stream: S) -> Result<(), ServiceError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut connection = Connection::new(
            stream,
            Config::smsc(self.system_id.clone()),
            ConnectionOptions::default(),
        );
        let mut shutdown = self.shutdown.subscribe();
        let (site, binding) = loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let action = tokio::select! {
                event = connection.next() => event?,
                _ = shutdown.changed() => { connection.shutdown().await?; return Ok(()); }
            };
            let Some(action) = action else {
                return Err(ServiceError::ClosedBeforeBind);
            };
            if let Action::BindRequest {
                sequence,
                system_id,
                password,
            } = action
            {
                match self.authenticate(&system_id, &password).await {
                    Some(site) => {
                        let Some(binding) = self.bound.claim(site.id) else {
                            connection
                                .reject_bind(sequence, CommandStatus::AlreadyBound)
                                .await?;
                            return Err(ServiceError::AlreadyBound(site.id));
                        };
                        connection.accept_bind(sequence, system_id).await?;
                        break (site, binding);
                    }
                    None => {
                        connection
                            .reject_bind(sequence, CommandStatus::BindFailed)
                            .await?;
                        return Err(ServiceError::BindRefused(system_id));
                    }
                }
            }
        };
        tracing::info!(site = %site.id, name = %site.name, "site bound to the hub");
        self.run(connection, site, binding).await
    }

    pub async fn serve_outbound<S>(&self, stream: S, site: Site) -> Result<(), ServiceError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let binding = self
            .bound
            .claim(site.id)
            .ok_or(ServiceError::AlreadyBound(site.id))?;
        let password = self.registry.smpp_password(site.id).await?;
        let mut connection = Connection::new(
            stream,
            Config::esme(site.smpp_system_id.clone(), password),
            ConnectionOptions::default(),
        );
        let mut shutdown = self.shutdown.subscribe();
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let action = tokio::select! {
                event = connection.next() => event?,
                _ = shutdown.changed() => { connection.shutdown().await?; return Ok(()); }
            };
            let Some(action) = action else {
                return Err(ServiceError::ClosedBeforeBind);
            };
            if matches!(action, Action::Bound { .. }) {
                break;
            }
        }
        tracing::info!(site = %site.id, name = %site.name, "hub bound to a site");
        self.run(connection, site, binding).await
    }

    async fn authenticate(&self, system_id: &str, password: &str) -> Option<Site> {
        let site = self.registry.site_by_system_id(system_id).await.ok()??;
        let stored = self.registry.smpp_password(site.id).await.ok()?;
        constant_time_eq(stored.as_bytes(), password.as_bytes()).then_some(site)
    }

    async fn run(
        &self,
        mut session: Connection,
        site: Site,
        mut binding: BoundSession,
    ) -> Result<(), ServiceError> {
        let mut shutdown = self.shutdown.subscribe();
        let mut in_flight: HashMap<u32, MessageId> = HashMap::new();
        let result = async {
            if *shutdown.borrow() { session.shutdown().await?; return Ok(()); }
            self.deliver_due(&mut session, &site, &mut in_flight)
                .await?;
            let mut ticker = tokio::time::interval(DRAIN_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => { session.shutdown().await?; return Ok(()); }
                    event = session.next() => {
                        let Some(action) = event? else { return Ok(()); };
                        if self.apply(action, &mut session, &site, &mut in_flight).await? { return Ok(()); }
                    }
                    _ = binding.nudges.recv() => {
                        self.deliver_due(&mut session, &site, &mut in_flight).await?;
                    }
                    _ = ticker.tick() => {
                        self.deliver_due(&mut session, &site, &mut in_flight).await?;
                    }
                }
            }
        }
        .await;

        for id in in_flight.into_values() {
            self.record(
                id,
                Outcome::Retry {
                    error: "the session closed before the site answered".into(),
                },
            )
            .await;
        }
        result
    }

    async fn apply(
        &self,
        action: Action,
        session: &mut Connection,
        site: &Site,
        in_flight: &mut HashMap<u32, MessageId>,
    ) -> Result<bool, ServiceError> {
        match action {
            Action::Send(pdu) => session.send(pdu).await?,
            Action::Message { sequence, body } => {
                let reply = if body.is_delivery_receipt() {
                    self.take_receipt(session, site, sequence, *body).await
                } else {
                    self.take_submission(session, site, sequence, *body).await
                };
                session.send(reply).await?;
            }
            Action::MessageAck {
                sequence,
                status,
                message_id,
            } => {
                if let Some(id) = in_flight.remove(&sequence) {
                    let outcome = if status == CommandStatus::Ok {
                        if !message_id.is_empty() {
                            self.registry
                                .record_smpp_destination(id, &message_id)
                                .await?;
                        }
                        Outcome::Delivered
                    } else if is_permanent(status) {
                        Outcome::Failed {
                            error: format!("the site refused it: {status:?}"),
                        }
                    } else {
                        Outcome::Retry {
                            error: format!("the site could not take it: {status:?}"),
                        }
                    };
                    self.record(id, outcome).await;
                    self.deliver_due(session, site, in_flight).await?;
                }
            }
            Action::RequestTimeout { sequence } => {
                if let Some(id) = in_flight.remove(&sequence) {
                    self.record(
                        id,
                        Outcome::Retry {
                            error: "the site did not answer in time".into(),
                        },
                    )
                    .await;
                    self.deliver_due(session, site, in_flight).await?;
                }
            }
            Action::Closed => return Ok(true),
            Action::Bound { .. } | Action::BindRequest { .. } => {}
        }
        Ok(false)
    }

    async fn take_submission(
        &self,
        session: &Connection,
        site: &Site,
        sequence: u32,
        body: MessageBody,
    ) -> Pdu {
        let source = body.source.digits.clone();
        let destination = body.destination.digits.clone();
        match self.accept(site, &source, &destination, body).await {
            Ok(id) => {
                tracing::info!(%source, %destination, %id, "hub took a message");
                self.bound.nudge_all().await;
                session.respond_to_message(sequence, CommandStatus::Ok, id.to_string())
            }
            Err(rejection) => {
                tracing::warn!(%source, %destination, site = %site.id, "refused: {rejection}");
                session.respond_to_message(sequence, rejection.status(), String::new())
            }
        }
    }

    async fn take_receipt(
        &self,
        session: &Connection,
        site: &Site,
        sequence: u32,
        body: MessageBody,
    ) -> Pdu {
        let result = async {
            let receipt = receipt::parse(&body).ok_or(ServiceError::ClosedBeforeBind)?;
            let original = self
                .registry
                .smpp_receipted_message(site.id, &receipt.message_id)
                .await?
                .ok_or(ServiceError::ClosedBeforeBind)?;
            if session.role() != smpp_core::Role::Esme
                || body.source.digits != original.to_number
                || body.destination.digits != original.from_number
            {
                return Err(ServiceError::ClosedBeforeBind);
            }
            let state = receipt.state;
            if !smpp_core::pdu::message_state::is_final(state) {
                return Ok::<(), ServiceError>(());
            }
            self.registry
                .record_handset_receipt(original.id, state)
                .await?;
            self.queue_receipt(original, state).await?;
            Ok(())
        }
        .await;
        session.respond_to_message(
            sequence,
            match result {
                Ok(()) => CommandStatus::Ok,
                Err(ServiceError::ClosedBeforeBind) => CommandStatus::InvalidDestAddress,
                Err(_) => CommandStatus::SystemError,
            },
            String::new(),
        )
    }

    async fn queue_receipt(
        &self,
        original: crate::model::QueuedMessage,
        state: u8,
    ) -> Result<(), ServiceError> {
        let origin = self
            .registry
            .site(original.origin_site)
            .await?
            .ok_or(ServiceError::ClosedBeforeBind)?;
        if original.receipt_requested
            && !(original.receipt_failure_only && state == smpp_core::pdu::message_state::DELIVERED)
            && origin.bind_role == BindRole::SiteBindsToHub
        {
            let receipt = MessageBody {
                source: Address::unknown(&original.to_number),
                destination: Address::unknown(&original.from_number),
                esm_class: smpp_core::esm_class::DELIVERY_RECEIPT,
                receipted_message_id: Some(original.id.to_string()),
                message_state: Some(state),
                ..MessageBody::default()
            };
            self.registry
                .accept_message(NewMessage {
                    origin_site: original.destination_site,
                    destination_site: original.origin_site,
                    from_number: original.to_number,
                    to_number: original.from_number,
                    text: String::new(),
                    bearer_data: None,
                    teleservice: None,
                    receipt_requested: false,
                    receipt_failure_only: false,
                    received_at: Utc::now(),
                    expires_at: expiry(Utc::now(), self.quotas().await?.message_ttl_hours),
                    receipt_for: Some(original.id),
                    smpp_pdu: Some(smpp_core::encode(&Pdu::request(
                        1,
                        smpp_core::Body::DeliverSm(Box::new(receipt)),
                    ))?),
                })
                .await?;
            self.bound.nudge_all().await;
        }
        Ok(())
    }

    pub async fn queue_terminal_receipts(&self) -> Result<(), ServiceError> {
        for message in self.registry.pending_terminal_receipts().await? {
            let state = if message.state == crate::model::MessageState::Expired {
                smpp_core::pdu::message_state::EXPIRED
            } else {
                smpp_core::pdu::message_state::UNDELIVERABLE
            };
            self.queue_receipt(message, state).await?;
        }
        Ok(())
    }

    async fn accept(
        &self,
        site: &Site,
        source: &str,
        destination: &str,
        body: MessageBody,
    ) -> Result<MessageId, Rejection> {
        let quotas = self.quotas().await.map_err(storage_rejection)?;
        let now = Utc::now();
        let rate_limit = site
            .messages_per_hour
            .unwrap_or(quotas.messages_per_site_hour);

        let source_owner = self
            .registry
            .site_for_number(source)
            .await
            .map_err(storage_rejection)?;
        let destination_owner = self
            .registry
            .site_for_number(destination)
            .await
            .map_err(storage_rejection)?;
        let sent_this_hour = self
            .registry
            .sent_since(site.id, now - chrono::Duration::hours(1))
            .await
            .map_err(storage_rejection)?;
        let queued_for_destination = match destination_owner {
            Some(id) => self
                .registry
                .queued_for_site(id)
                .await
                .map_err(storage_rejection)?,
            None => 0,
        };

        let authorization = authorize(&Lookup {
            sending_site: site.id,
            source_owner,
            destination_owner,
            source,
            destination,
            reserved_destination: self.registry.is_reserved(destination),
            sent_this_hour,
            rate_limit,
            queued_for_destination,
            queue_limit: QUEUE_PER_SITE,
        })?;

        self.registry
            .accept_message(NewMessage {
                origin_site: authorization.origin_site,
                destination_site: authorization.destination_site,
                from_number: source.to_string(),
                to_number: destination.to_string(),
                smpp_pdu: Some(
                    smpp_core::encode(&Pdu::request(
                        1,
                        smpp_core::Body::SubmitSm(Box::new(body.clone())),
                    ))
                    .map_err(|_| Rejection::UnknownDestination(destination.to_string()))?,
                ),
                receipt_for: None,
                text: smpp_core::text::decode(&body).unwrap_or_default(),
                bearer_data: body.bearer_data,
                teleservice: body.teleservice,
                receipt_requested: body.registered_delivery & smpp_core::pdu::DELIVERY_RECEIPT_MASK
                    != 0,
                receipt_failure_only: body.registered_delivery
                    & smpp_core::pdu::DELIVERY_RECEIPT_MASK
                    == smpp_core::pdu::DELIVERY_RECEIPT_FAILURE_ONLY,
                received_at: now,
                expires_at: expiry(now, quotas.message_ttl_hours),
            })
            .await
            .map_err(storage_rejection)
    }

    async fn deliver_due(
        &self,
        session: &mut Connection,
        site: &Site,
        in_flight: &mut HashMap<u32, MessageId>,
    ) -> Result<(), ServiceError> {
        let room = DELIVERY_WINDOW.saturating_sub(in_flight.len());
        if room == 0 {
            return Ok(());
        }
        let due = self
            .registry
            .due_messages(site.id, Utc::now(), DELIVERY_WINDOW as u32)
            .await?;
        for message in due {
            if in_flight.values().any(|id| *id == message.id) {
                continue;
            }
            if in_flight.len() == DELIVERY_WINDOW {
                break;
            }
            let mut body = if let Some(bytes) = &message.smpp_pdu {
                match smpp_core::decode(bytes)?.body {
                    smpp_core::Body::SubmitSm(body) | smpp_core::Body::DeliverSm(body) => *body,
                    _ => return Err(ServiceError::ClosedBeforeBind),
                }
            } else {
                MessageBody {
                    source: Address::unknown(&message.from_number),
                    destination: Address::unknown(&message.to_number),
                    text: message.text.clone().into_bytes(),
                    bearer_data: message.bearer_data.clone(),
                    teleservice: message.teleservice,
                    ..MessageBody::default()
                }
            };
            if !body.is_delivery_receipt() && session.role() == smpp_core::Role::Esme {
                body.registered_delivery |= smpp_core::REGISTERED_DELIVERY_RECEIPT;
            }
            match session.send_message(body).await {
                Ok(sequence) => {
                    in_flight.insert(sequence, message.id);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn record(&self, id: MessageId, outcome: Outcome) {
        let message = match self.registry.message(id).await {
            Ok(Some(message)) => message,
            _ => return,
        };
        let resolved = resolve(
            &self.schedule,
            outcome,
            message.attempts,
            Utc::now(),
            message.expires_at,
        );
        if let Err(err) = self.registry.complete_message(id, resolved).await {
            tracing::error!("could not record the outcome for {id}: {err}");
        }
    }

    async fn quotas(&self) -> Result<Quotas, crate::store::StoreError> {
        self.registry.quotas().await
    }
}

fn storage_rejection(error: crate::store::StoreError) -> Rejection {
    tracing::error!(%error, "SMS submission storage operation failed");
    Rejection::StorageUnavailable
}

pub const QUEUE_PER_SITE: u32 = 1_000;

fn is_permanent(status: CommandStatus) -> bool {
    matches!(
        status,
        CommandStatus::InvalidDestAddress
            | CommandStatus::InvalidSourceAddress
            | CommandStatus::InvalidMsgLength
    )
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn hub_dials(site: &Site) -> bool {
    site.bind_role == BindRole::HubBindsToSite
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("site {0} already has an active SMPP session")]
    AlreadyBound(SiteId),
    #[error("the peer closed before completing a bind")]
    ClosedBeforeBind,
    #[error("refused a bind from {0}")]
    BindRefused(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Smpp(#[from] smpp_core::Error),
    #[error("{0}")]
    Store(#[from] crate::store::StoreError),
}

#[cfg(test)]
mod tests;
