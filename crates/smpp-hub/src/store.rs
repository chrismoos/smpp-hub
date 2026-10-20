use crate::model::{MessageId, MessageState, QueuedMessage, Quotas, Site, SiteId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StoreError(pub String);

#[async_trait]
pub trait Store: Send + Sync {
    fn is_reserved(&self, number: &str) -> bool;
    async fn quotas(&self) -> Result<Quotas, StoreError>;
    async fn site(&self, id: SiteId) -> Result<Option<Site>, StoreError>;
    async fn site_by_system_id(&self, system_id: &str) -> Result<Option<Site>, StoreError>;
    async fn smpp_password(&self, id: SiteId) -> Result<String, StoreError>;
    async fn site_for_number(&self, number: &str) -> Result<Option<SiteId>, StoreError>;
    async fn pending_terminal_receipts(&self) -> Result<Vec<QueuedMessage>, StoreError>;
    async fn record_handset_receipt(&self, id: MessageId, state: u8) -> Result<(), StoreError>;
    async fn record_smpp_destination(
        &self,
        id: MessageId,
        remote_id: &str,
    ) -> Result<(), StoreError>;
    async fn smpp_receipted_message(
        &self,
        site: SiteId,
        remote_id: &str,
    ) -> Result<Option<QueuedMessage>, StoreError>;

    /// Commit before returning. Repeated nonempty receipt_for values return the existing receipt ID.
    async fn accept_message(&self, message: NewMessage) -> Result<MessageId, StoreError>;

    async fn message(&self, id: MessageId) -> Result<Option<QueuedMessage>, StoreError>;

    async fn due_messages(
        &self,
        site: SiteId,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<QueuedMessage>, StoreError>;

    async fn queued_for_site(&self, site: SiteId) -> Result<u32, StoreError>;

    async fn sent_since(&self, site: SiteId, since: DateTime<Utc>) -> Result<u32, StoreError>;

    /// Terminal outcomes must clear text, bearer data, and the encoded PDU in the same commit.
    async fn complete_message(
        &self,
        id: MessageId,
        outcome: MessageOutcome,
    ) -> Result<(), StoreError>;

    async fn expire_messages(&self, now: DateTime<Utc>) -> Result<Vec<MessageId>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct NewMessage {
    pub smpp_pdu: Option<Vec<u8>>,
    pub receipt_for: Option<MessageId>,
    pub origin_site: SiteId,
    pub destination_site: SiteId,
    pub from_number: String,
    pub to_number: String,
    pub text: String,
    pub bearer_data: Option<Vec<u8>>,
    pub teleservice: Option<u16>,
    pub receipt_requested: bool,
    pub receipt_failure_only: bool,
    pub received_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MessageOutcome {
    pub state: MessageState,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}
