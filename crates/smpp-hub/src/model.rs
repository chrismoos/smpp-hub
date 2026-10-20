use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use uuid::Uuid;
macro_rules! uuid_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, f)
            }
        }
    };
}

uuid_newtype!(SiteId, "SMPP peer identifier.");
uuid_newtype!(MessageId, "Queued message identifier.");
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum BindRole {
    HubBindsToSite,
    SiteBindsToHub,
}

impl BindRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HubBindsToSite => "hub_binds_to_site",
            Self::SiteBindsToHub => "site_binds_to_hub",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "hub_binds_to_site" => Self::HubBindsToSite,
            "site_binds_to_hub" => Self::SiteBindsToHub,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedMessage {
    pub smpp_pdu: Option<Vec<u8>>,
    pub receipt_for: Option<MessageId>,
    pub id: MessageId,
    pub origin_site: SiteId,
    pub destination_site: SiteId,
    pub from_number: String,
    pub to_number: String,
    pub text: String,
    pub bearer_data: Option<Vec<u8>>,
    pub teleservice: Option<u16>,
    pub receipt_requested: bool,
    pub receipt_failure_only: bool,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub state: MessageState,
    pub attempts: u32,
    pub next_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: Option<String>,
}

impl QueuedMessage {
    pub fn is_due(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.state == MessageState::Queued
            && self.next_attempt_at.map(|at| at <= now).unwrap_or(true)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Site {
    pub id: SiteId,
    pub name: String,
    pub smpp_system_id: String,
    pub bind_role: BindRole,
    pub messages_per_hour: Option<u32>,
}

pub const DEFAULT_MESSAGE_TTL_HOURS: u32 = 24;
pub const DEFAULT_MESSAGES_PER_SITE_HOUR: u32 = 600;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct Quotas {
    pub message_ttl_hours: u32,
    pub messages_per_site_hour: u32,
}

impl Default for Quotas {
    fn default() -> Self {
        Self {
            message_ttl_hours: DEFAULT_MESSAGE_TTL_HOURS,
            messages_per_site_hour: DEFAULT_MESSAGES_PER_SITE_HOUR,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    Queued,
    Delivered,
    Expired,
    Failed,
}

impl MessageState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered => "delivered",
            Self::Expired => "expired",
            Self::Failed => "failed",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "queued" => Self::Queued,
            "delivered" => Self::Delivered,
            "expired" => Self::Expired,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}
