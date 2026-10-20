use super::*;
use crate::model::QueuedMessage;
use crate::store::{MessageOutcome, StoreError};
use chrono::DateTime;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LookupFailure {
    Quotas,
    SourceOwner,
    DestinationOwner,
    RateUsage,
    QueueUsage,
}

struct FailingStore {
    failure: Option<LookupFailure>,
    accepted: AtomicUsize,
}

impl FailingStore {
    fn check(&self, operation: LookupFailure) -> Result<(), StoreError> {
        if self.failure == Some(operation) {
            Err(StoreError("injected database failure".into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl Store for FailingStore {
    fn is_reserved(&self, _: &str) -> bool {
        false
    }
    async fn quotas(&self) -> Result<Quotas, StoreError> {
        self.check(LookupFailure::Quotas)?;
        Ok(Quotas::default())
    }
    async fn site_for_number(&self, number: &str) -> Result<Option<SiteId>, StoreError> {
        let (operation, id) = if number == "1000" {
            (LookupFailure::SourceOwner, 1)
        } else {
            (LookupFailure::DestinationOwner, 2)
        };
        self.check(operation)?;
        Ok(Some(SiteId(uuid::Uuid::from_u128(id))))
    }
    async fn sent_since(&self, _: SiteId, _: DateTime<Utc>) -> Result<u32, StoreError> {
        self.check(LookupFailure::RateUsage)?;
        Ok(0)
    }
    async fn queued_for_site(&self, _: SiteId) -> Result<u32, StoreError> {
        self.check(LookupFailure::QueueUsage)?;
        Ok(0)
    }
    async fn accept_message(&self, _: NewMessage) -> Result<MessageId, StoreError> {
        self.accepted.fetch_add(1, Ordering::SeqCst);
        Ok(MessageId::new())
    }
    async fn site(&self, _: SiteId) -> Result<Option<Site>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn site_by_system_id(&self, _: &str) -> Result<Option<Site>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn smpp_password(&self, _: SiteId) -> Result<String, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn pending_terminal_receipts(&self) -> Result<Vec<QueuedMessage>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn record_handset_receipt(&self, _: MessageId, _: u8) -> Result<(), StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn record_smpp_destination(&self, _: MessageId, _: &str) -> Result<(), StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn smpp_receipted_message(
        &self,
        _: SiteId,
        _: &str,
    ) -> Result<Option<QueuedMessage>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn message(&self, _: MessageId) -> Result<Option<QueuedMessage>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn due_messages(
        &self,
        _: SiteId,
        _: DateTime<Utc>,
        _: u32,
    ) -> Result<Vec<QueuedMessage>, StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn complete_message(&self, _: MessageId, _: MessageOutcome) -> Result<(), StoreError> {
        unreachable!("not used by submission checks")
    }
    async fn expire_messages(&self, _: DateTime<Utc>) -> Result<Vec<MessageId>, StoreError> {
        unreachable!("not used by submission checks")
    }
}

#[tokio::test]
async fn storage_lookup_failures_are_retryable_and_never_accept_messages() {
    let site = Site {
        id: SiteId(uuid::Uuid::from_u128(1)),
        name: "origin".into(),
        smpp_system_id: "origin".into(),
        bind_role: BindRole::SiteBindsToHub,
        messages_per_hour: None,
    };
    for failure in [
        None,
        Some(LookupFailure::Quotas),
        Some(LookupFailure::SourceOwner),
        Some(LookupFailure::DestinationOwner),
        Some(LookupFailure::RateUsage),
        Some(LookupFailure::QueueUsage),
    ] {
        let store = Arc::new(FailingStore {
            failure,
            accepted: AtomicUsize::new(0),
        });
        let hub = Hub::new(store.clone(), "test-hub");
        let body = MessageBody {
            source: Address::unknown("1000"),
            destination: Address::unknown("1001"),
            ..MessageBody::default()
        };
        let result = hub.accept(&site, "1000", "1001", body).await;
        if failure.is_none() {
            assert!(result.is_ok());
            assert_eq!(store.accepted.load(Ordering::SeqCst), 1);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error, Rejection::StorageUnavailable, "{failure:?}");
            assert_eq!(error.status(), CommandStatus::SystemError);
            assert!(error.status().is_retryable());
            assert_eq!(store.accepted.load(Ordering::SeqCst), 0);
        }
    }
}
