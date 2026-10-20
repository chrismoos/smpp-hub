use std::{collections::HashSet, path::Path};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};

use crate::{model::*, store::*};

#[derive(Clone, Serialize, Deserialize)]
pub struct Peer {
    #[serde(flatten)]
    pub site: Site,
    pub password: String,
    pub numbers: Vec<String>,
    pub address: Option<std::net::SocketAddr>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Settings {
    pub listen: std::net::SocketAddr,
    pub system_id: String,
    pub database: std::path::PathBuf,
    pub peers: Vec<Peer>,
    #[serde(default)]
    pub reserved_numbers: Vec<String>,
    #[serde(default)]
    pub quotas: Quotas,
}

pub struct SqliteStore {
    pool: SqlitePool,
    settings: Settings,
}

fn failure(error: impl std::fmt::Display) -> StoreError {
    StoreError(error.to_string())
}

impl SqliteStore {
    pub async fn open(settings: Settings) -> Result<Self, StoreError> {
        let mut ids = HashSet::new();
        let mut systems = HashSet::new();
        let mut numbers = HashSet::new();
        for peer in &settings.peers {
            let now = std::time::Instant::now();
            let mut session = smpp_core::Session::new(
                smpp_core::Config::esme(&peer.site.smpp_system_id, &peer.password),
                now,
            );
            smpp_core::encode(&session.bind(now).map_err(failure)?).map_err(failure)?;
            if peer.address == Some(settings.listen) {
                return Err(StoreError("peer address points at the hub listener".into()));
            }
            if !ids.insert(peer.site.id)
                || !systems.insert(&peer.site.smpp_system_id)
                || peer.password.is_empty()
                || peer.site.smpp_system_id.is_empty()
                || (peer.site.bind_role == BindRole::HubBindsToSite && peer.address.is_none())
            {
                return Err(StoreError(
                    "invalid or duplicate SMPP peer configuration".into(),
                ));
            }
            for number in &peer.numbers {
                if number.is_empty()
                    || !number.bytes().all(|b| b.is_ascii_digit())
                    || !numbers.insert(number)
                    || settings.reserved_numbers.contains(number)
                {
                    return Err(StoreError(
                        "invalid, reserved, or duplicate number route".into(),
                    ));
                }
            }
        }
        let options = SqliteConnectOptions::new()
            .filename(Path::new(&settings.database))
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Full);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(failure)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(failure)?;
        Ok(Self { pool, settings })
    }

    async fn messages(&self) -> Result<Vec<QueuedMessage>, StoreError> {
        sqlx::query_scalar::<_, String>("SELECT payload FROM messages ORDER BY received_at, id")
            .fetch_all(&self.pool)
            .await
            .map_err(failure)?
            .into_iter()
            .map(|s| serde_json::from_str(&s).map_err(failure))
            .collect()
    }

    async fn replace(&self, message: &QueuedMessage) -> Result<(), StoreError> {
        let payload = serde_json::to_string(message).map_err(failure)?;
        sqlx::query("UPDATE messages SET payload=? WHERE id=?")
            .bind(payload)
            .bind(message.id.to_string())
            .execute(&self.pool)
            .await
            .map_err(failure)?;
        Ok(())
    }
}

#[async_trait]
impl Store for SqliteStore {
    fn is_reserved(&self, number: &str) -> bool {
        self.settings.reserved_numbers.iter().any(|n| n == number)
    }
    async fn quotas(&self) -> Result<Quotas, StoreError> {
        Ok(self.settings.quotas)
    }
    async fn site(&self, id: SiteId) -> Result<Option<Site>, StoreError> {
        Ok(self
            .settings
            .peers
            .iter()
            .find(|p| p.site.id == id)
            .map(|p| p.site.clone()))
    }
    async fn site_by_system_id(&self, system_id: &str) -> Result<Option<Site>, StoreError> {
        Ok(self
            .settings
            .peers
            .iter()
            .find(|p| p.site.smpp_system_id == system_id)
            .map(|p| p.site.clone()))
    }
    async fn smpp_password(&self, id: SiteId) -> Result<String, StoreError> {
        self.settings
            .peers
            .iter()
            .find(|p| p.site.id == id)
            .map(|p| p.password.clone())
            .ok_or_else(|| StoreError("unknown SMPP peer".into()))
    }
    async fn site_for_number(&self, number: &str) -> Result<Option<SiteId>, StoreError> {
        Ok(self
            .settings
            .peers
            .iter()
            .find(|p| p.numbers.iter().any(|n| n == number))
            .map(|p| p.site.id))
    }
    async fn accept_message(&self, m: NewMessage) -> Result<MessageId, StoreError> {
        let message = QueuedMessage {
            id: MessageId::new(),
            smpp_pdu: m.smpp_pdu,
            receipt_for: m.receipt_for,
            origin_site: m.origin_site,
            destination_site: m.destination_site,
            from_number: m.from_number,
            to_number: m.to_number,
            text: m.text,
            bearer_data: m.bearer_data,
            teleservice: m.teleservice,
            receipt_requested: m.receipt_requested,
            receipt_failure_only: m.receipt_failure_only,
            received_at: m.received_at,
            expires_at: m.expires_at,
            state: MessageState::Queued,
            attempts: 0,
            next_attempt_at: None,
            last_error: None,
        };
        let payload = serde_json::to_string(&message).map_err(failure)?;
        let receipt_for = message.receipt_for.map(|id| id.to_string());
        let result = sqlx::query("INSERT INTO messages(id, received_at, receipt_for, payload) VALUES(?,?,?,?) ON CONFLICT(receipt_for) DO NOTHING")
            .bind(message.id.to_string()).bind(message.received_at.timestamp_millis()).bind(&receipt_for)
            .bind(payload).execute(&self.pool).await.map_err(failure)?;
        if result.rows_affected() == 0 {
            let id: String = sqlx::query_scalar("SELECT id FROM messages WHERE receipt_for=?")
                .bind(receipt_for)
                .fetch_one(&self.pool)
                .await
                .map_err(failure)?;
            return uuid::Uuid::parse_str(&id).map(MessageId).map_err(failure);
        }
        Ok(message.id)
    }
    async fn message(&self, id: MessageId) -> Result<Option<QueuedMessage>, StoreError> {
        let payload: Option<String> = sqlx::query_scalar("SELECT payload FROM messages WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(failure)?;
        payload
            .map(|s| serde_json::from_str(&s).map_err(failure))
            .transpose()
    }
    async fn due_messages(
        &self,
        site: SiteId,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<QueuedMessage>, StoreError> {
        Ok(self
            .messages()
            .await?
            .into_iter()
            .filter(|m| m.destination_site == site && m.expires_at > now && m.is_due(now))
            .take(limit as usize)
            .collect())
    }
    async fn queued_for_site(&self, site: SiteId) -> Result<u32, StoreError> {
        Ok(self
            .messages()
            .await?
            .iter()
            .filter(|m| m.destination_site == site && m.state == MessageState::Queued)
            .count() as u32)
    }
    async fn sent_since(&self, site: SiteId, since: DateTime<Utc>) -> Result<u32, StoreError> {
        Ok(self
            .messages()
            .await?
            .iter()
            .filter(|m| m.origin_site == site && m.received_at >= since && m.receipt_for.is_none())
            .count() as u32)
    }
    async fn complete_message(
        &self,
        id: MessageId,
        outcome: MessageOutcome,
    ) -> Result<(), StoreError> {
        let mut message = self
            .message(id)
            .await?
            .ok_or_else(|| StoreError("unknown message".into()))?;
        message.state = outcome.state;
        message.next_attempt_at = outcome.next_attempt_at;
        message.last_error = outcome.last_error;
        message.attempts += 1;
        if message.state != MessageState::Queued {
            message.text.clear();
            message.bearer_data = None;
            message.smpp_pdu = None;
        }
        self.replace(&message).await
    }
    async fn expire_messages(&self, now: DateTime<Utc>) -> Result<Vec<MessageId>, StoreError> {
        let mut expired = Vec::new();
        for message in self.messages().await? {
            if message.state == MessageState::Queued && message.expires_at <= now {
                self.complete_message(
                    message.id,
                    MessageOutcome {
                        state: MessageState::Expired,
                        next_attempt_at: None,
                        last_error: Some("message expired".into()),
                    },
                )
                .await?;
                expired.push(message.id);
            }
        }
        Ok(expired)
    }
    async fn record_smpp_destination(
        &self,
        id: MessageId,
        remote_id: &str,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE messages SET remote_id=? WHERE id=?")
            .bind(remote_id)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(failure)?;
        Ok(())
    }
    async fn smpp_receipted_message(
        &self,
        site: SiteId,
        remote_id: &str,
    ) -> Result<Option<QueuedMessage>, StoreError> {
        let rows = sqlx::query("SELECT payload FROM messages WHERE remote_id=?")
            .bind(remote_id)
            .fetch_all(&self.pool)
            .await
            .map_err(failure)?;
        for row in rows {
            let m: QueuedMessage =
                serde_json::from_str(row.get::<&str, _>("payload")).map_err(failure)?;
            if m.destination_site == site {
                return Ok(Some(m));
            }
        }
        Ok(None)
    }
    async fn record_handset_receipt(&self, id: MessageId, state: u8) -> Result<(), StoreError> {
        sqlx::query("UPDATE messages SET handset_state=? WHERE id=?")
            .bind(state)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(failure)?;
        Ok(())
    }
    async fn pending_terminal_receipts(&self) -> Result<Vec<QueuedMessage>, StoreError> {
        let messages = self.messages().await?;
        Ok(messages
            .iter()
            .filter(|m| {
                m.receipt_requested
                    && matches!(m.state, MessageState::Failed | MessageState::Expired)
                    && !messages.iter().any(|r| r.receipt_for == Some(m.id))
            })
            .cloned()
            .collect())
    }
}
