use crate::model::SiteId;
use smpp_core::CommandStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    pub origin_site: SiteId,
    pub destination_site: SiteId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    StorageUnavailable,
    SourceNotOwned { source: String, site: SiteId },
    UnknownDestination(String),
    ReservedDestination(String),
    Throttled { site: SiteId },
    QueueFull { site: SiteId },
}

impl Rejection {
    pub fn status(&self) -> CommandStatus {
        match self {
            Self::StorageUnavailable => CommandStatus::SystemError,
            Self::SourceNotOwned { .. } => CommandStatus::InvalidSourceAddress,
            Self::UnknownDestination(_) | Self::ReservedDestination(_) => {
                CommandStatus::InvalidDestAddress
            }
            Self::Throttled { .. } => CommandStatus::Throttled,
            Self::QueueFull { .. } => CommandStatus::MessageQueueFull,
        }
    }
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageUnavailable => f.write_str("message storage is temporarily unavailable"),
            Self::SourceNotOwned { source, site } => {
                write!(f, "site {site} does not own the number {source}")
            }
            Self::UnknownDestination(number) => write!(f, "{number} is not on this network"),
            Self::ReservedDestination(number) => {
                write!(f, "{number} is reserved and never routes")
            }
            Self::Throttled { site } => write!(f, "site {site} is over its message rate"),
            Self::QueueFull { site } => write!(f, "the queue for site {site} is full"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Lookup<'a> {
    pub sending_site: SiteId,
    pub source_owner: Option<SiteId>,
    pub destination_owner: Option<SiteId>,
    pub source: &'a str,
    pub destination: &'a str,
    pub reserved_destination: bool,
    pub sent_this_hour: u32,
    pub rate_limit: u32,
    pub queued_for_destination: u32,
    pub queue_limit: u32,
}

pub fn authorize(lookup: &Lookup<'_>) -> Result<Authorization, Rejection> {
    if lookup.source_owner != Some(lookup.sending_site) {
        return Err(Rejection::SourceNotOwned {
            source: lookup.source.to_string(),
            site: lookup.sending_site,
        });
    }
    if lookup.reserved_destination {
        return Err(Rejection::ReservedDestination(
            lookup.destination.to_string(),
        ));
    }
    let Some(destination_site) = lookup.destination_owner else {
        return Err(Rejection::UnknownDestination(
            lookup.destination.to_string(),
        ));
    };
    if lookup.sent_this_hour >= lookup.rate_limit {
        return Err(Rejection::Throttled {
            site: lookup.sending_site,
        });
    }
    if lookup.queued_for_destination >= lookup.queue_limit {
        return Err(Rejection::QueueFull {
            site: destination_site,
        });
    }
    Ok(Authorization {
        origin_site: lookup.sending_site,
        destination_site,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(n: u128) -> SiteId {
        SiteId(uuid::Uuid::from_u128(n))
    }

    fn lookup<'a>(source: &'a str, destination: &'a str) -> Lookup<'a> {
        Lookup {
            sending_site: site(1),
            source_owner: Some(site(1)),
            destination_owner: Some(site(2)),
            source,
            destination,
            reserved_destination: false,
            sent_this_hour: 0,
            rate_limit: 600,
            queued_for_destination: 0,
            queue_limit: 1000,
        }
    }

    #[test]
    fn a_message_from_a_number_the_site_owns_is_accepted() {
        let auth = authorize(&lookup("7025550100", "4155550118")).unwrap();
        assert_eq!(auth.origin_site, site(1));
        assert_eq!(auth.destination_site, site(2));
    }

    #[test]
    fn a_site_cannot_send_as_someone_elses_subscriber() {
        let mut l = lookup("4155550118", "7025550100");
        l.source_owner = Some(site(2));
        let err = authorize(&l).unwrap_err();
        assert!(matches!(err, Rejection::SourceNotOwned { .. }));
        assert_eq!(err.status(), CommandStatus::InvalidSourceAddress);
    }

    #[test]
    fn a_source_nobody_owns_is_refused_too() {
        let mut l = lookup("9995550000", "4155550118");
        l.source_owner = None;
        assert!(matches!(
            authorize(&l).unwrap_err(),
            Rejection::SourceNotOwned { .. }
        ));
    }

    #[test]
    fn spoofing_is_reported_before_an_unknown_destination() {
        let mut l = lookup("4155550118", "9995550000");
        l.source_owner = Some(site(2));
        l.destination_owner = None;
        assert!(matches!(
            authorize(&l).unwrap_err(),
            Rejection::SourceNotOwned { .. }
        ));
    }

    #[test]
    fn an_unknown_destination_is_a_visible_failure() {
        let mut l = lookup("7025550100", "9995550000");
        l.destination_owner = None;
        let err = authorize(&l).unwrap_err();
        assert_eq!(
            err,
            Rejection::UnknownDestination("9995550000".into()),
            "this is what replaces the silent drop"
        );
        assert_eq!(err.status(), CommandStatus::InvalidDestAddress);
    }

    #[test]
    fn reserved_numbers_are_refused_at_routing_time() {
        for reserved in ["911", "112"] {
            let mut l = lookup("7025550100", reserved);
            l.reserved_destination = true;
            l.destination_owner = Some(site(2));
            assert_eq!(
                authorize(&l).unwrap_err(),
                Rejection::ReservedDestination(reserved.to_string()),
                "{reserved} must never appear to route"
            );
        }
    }

    #[test]
    fn a_site_over_its_rate_is_throttled_rather_than_suspended() {
        let mut l = lookup("7025550100", "4155550118");
        l.sent_this_hour = 600;
        let err = authorize(&l).unwrap_err();
        assert_eq!(err.status(), CommandStatus::Throttled);
        assert!(
            err.status().is_retryable(),
            "throttling has to tell the site to come back, not to give up"
        );
    }

    #[test]
    fn the_last_message_under_the_rate_still_goes() {
        let mut l = lookup("7025550100", "4155550118");
        l.sent_this_hour = 599;
        assert!(authorize(&l).is_ok());
    }

    #[test]
    fn a_full_queue_is_retryable() {
        let mut l = lookup("7025550100", "4155550118");
        l.queued_for_destination = 1000;
        let err = authorize(&l).unwrap_err();
        assert_eq!(err.status(), CommandStatus::MessageQueueFull);
        assert!(err.status().is_retryable());
    }

    #[test]
    fn address_failures_are_permanent_not_retryable() {
        let permanent = [
            Rejection::SourceNotOwned {
                source: "x".into(),
                site: site(1),
            },
            Rejection::UnknownDestination("x".into()),
            Rejection::ReservedDestination("911".into()),
        ];
        for rejection in permanent {
            assert!(
                !rejection.status().is_retryable(),
                "{rejection} must not invite a retry"
            );
        }
    }
}
