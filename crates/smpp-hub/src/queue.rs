use std::collections::HashMap;

use crate::model::{MessageId, MessageState, QueuedMessage, SiteId};
use crate::store::MessageOutcome;
use chrono::{DateTime, Utc};

use crate::schedule::{RetrySchedule, expiry, next_attempt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Delivered,
    Retry { error: String },
    Failed { error: String },
}

pub fn resolve(
    schedule: &RetrySchedule,
    outcome: Outcome,
    attempts_before: u32,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> MessageOutcome {
    match outcome {
        Outcome::Delivered => MessageOutcome {
            state: MessageState::Delivered,
            next_attempt_at: None,
            last_error: None,
        },
        Outcome::Failed { error } => MessageOutcome {
            state: MessageState::Failed,
            next_attempt_at: None,
            last_error: Some(error),
        },
        Outcome::Retry { error } => {
            match next_attempt(schedule, attempts_before + 1, now, expires_at) {
                Some(at) => MessageOutcome {
                    state: MessageState::Queued,
                    next_attempt_at: Some(at),
                    last_error: Some(error),
                },
                None => MessageOutcome {
                    state: MessageState::Expired,
                    next_attempt_at: None,
                    last_error: Some(error),
                },
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Queue {
    messages: HashMap<MessageId, QueuedMessage>,
    schedule: RetrySchedule,
    ttl_hours: u32,
}

impl Queue {
    pub fn new(ttl_hours: u32) -> Self {
        Self {
            messages: HashMap::new(),
            schedule: RetrySchedule::default(),
            ttl_hours,
        }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn get(&self, id: MessageId) -> Option<&QueuedMessage> {
        self.messages.get(&id)
    }

    pub fn accept(
        &mut self,
        origin_site: SiteId,
        destination_site: SiteId,
        from_number: String,
        to_number: String,
        text: String,
        bearer_data: Option<Vec<u8>>,
        teleservice: Option<u16>,
        receipt_requested: bool,
        now: DateTime<Utc>,
    ) -> MessageId {
        let id = MessageId::new();
        self.messages.insert(
            id,
            QueuedMessage {
                smpp_pdu: None,
                receipt_for: None,
                id,
                origin_site,
                destination_site,
                from_number,
                to_number,
                text,
                bearer_data,
                teleservice,
                receipt_requested,
                receipt_failure_only: false,
                received_at: now,
                expires_at: expiry(now, self.ttl_hours),
                state: MessageState::Queued,
                attempts: 0,
                next_attempt_at: None,
                last_error: None,
            },
        );
        id
    }

    pub fn due_for(&self, site: SiteId, now: DateTime<Utc>) -> Vec<&QueuedMessage> {
        let mut due: Vec<&QueuedMessage> = self
            .messages
            .values()
            .filter(|m| m.destination_site == site && m.is_due(now))
            .collect();
        due.sort_by_key(|m| m.received_at);
        due
    }

    pub fn queued_for(&self, site: SiteId) -> u32 {
        self.messages
            .values()
            .filter(|m| m.destination_site == site && m.state == MessageState::Queued)
            .count() as u32
    }

    pub fn complete(&mut self, id: MessageId, outcome: Outcome, now: DateTime<Utc>) {
        let schedule = self.schedule.clone();
        let Some(message) = self.messages.get_mut(&id) else {
            return;
        };
        message.attempts += 1;
        match outcome {
            Outcome::Delivered => {
                message.state = MessageState::Delivered;
                message.bearer_data = None;
                message.text.clear();
                message.next_attempt_at = None;
            }
            Outcome::Failed { error } => {
                message.state = MessageState::Failed;
                message.bearer_data = None;
                message.text.clear();
                message.last_error = Some(error);
                message.next_attempt_at = None;
            }
            Outcome::Retry { error } => {
                message.last_error = Some(error);
                match next_attempt(&schedule, message.attempts, now, message.expires_at) {
                    Some(at) => message.next_attempt_at = Some(at),
                    None => {
                        message.state = MessageState::Expired;
                        message.bearer_data = None;
                        message.text.clear();
                        message.next_attempt_at = None;
                    }
                }
            }
        }
    }

    pub fn sweep(&mut self, now: DateTime<Utc>) -> Vec<MessageId> {
        let mut expired = Vec::new();
        for message in self.messages.values_mut() {
            if message.state == MessageState::Queued && message.expires_at <= now {
                message.state = MessageState::Expired;
                message.bearer_data = None;
                message.text.clear();
                message.next_attempt_at = None;
                expired.push(message.id);
            }
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn site(n: u128) -> SiteId {
        SiteId(uuid::Uuid::from_u128(n))
    }

    fn queue_with_one(now: DateTime<Utc>) -> (Queue, MessageId) {
        let mut q = Queue::new(24);
        let id = q.accept(
            site(1),
            site(2),
            "7025550100".into(),
            "4155550118".into(),
            "on my way".into(),
            Some(vec![1, 2, 3]),
            Some(4098),
            true,
            now,
        );
        (q, id)
    }

    #[test]
    fn an_accepted_message_is_due_immediately() {
        let now = Utc::now();
        let (q, id) = queue_with_one(now);
        assert_eq!(q.due_for(site(2), now).len(), 1);
        assert_eq!(q.get(id).unwrap().state, MessageState::Queued);
    }

    #[test]
    fn a_message_is_only_due_for_its_own_destination() {
        let now = Utc::now();
        let (q, _) = queue_with_one(now);
        assert!(q.due_for(site(3), now).is_empty());
    }

    #[test]
    fn delivery_drops_the_payload() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        q.complete(id, Outcome::Delivered, now);
        let message = q.get(id).unwrap();
        assert_eq!(message.state, MessageState::Delivered);
        assert_eq!(message.bearer_data, None);
        assert!(
            message.text.is_empty(),
            "the hub must not keep content after delivery"
        );
        assert!(q.due_for(site(2), now).is_empty());
    }

    #[test]
    fn a_retry_backs_off_rather_than_spinning() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        q.complete(
            id,
            Outcome::Retry {
                error: "peer offline".into(),
            },
            now,
        );
        let message = q.get(id).unwrap();
        assert_eq!(message.attempts, 1);
        assert_eq!(message.next_attempt_at, Some(now + Duration::seconds(30)));
        assert!(
            q.due_for(site(2), now).is_empty(),
            "a message in backoff is not due yet"
        );
        assert_eq!(q.due_for(site(2), now + Duration::seconds(31)).len(), 1);
    }

    #[test]
    fn a_retry_keeps_the_payload_because_it_still_has_to_be_delivered() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        q.complete(
            id,
            Outcome::Retry {
                error: "offline".into(),
            },
            now,
        );
        assert_eq!(q.get(id).unwrap().bearer_data, Some(vec![1, 2, 3]));
    }

    #[test]
    fn a_permanent_refusal_stops_the_message_and_drops_the_payload() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        q.complete(
            id,
            Outcome::Failed {
                error: "unknown number".into(),
            },
            now,
        );
        let message = q.get(id).unwrap();
        assert_eq!(message.state, MessageState::Failed);
        assert_eq!(message.last_error.as_deref(), Some("unknown number"));
        assert_eq!(message.bearer_data, None);
    }

    #[test]
    fn a_retry_that_would_land_past_the_ttl_expires_instead() {
        let now = Utc::now();
        let mut q = Queue::new(24);
        let id = q.accept(
            site(1),
            site(2),
            "7025550100".into(),
            "4155550118".into(),
            "hi".into(),
            None,
            None,
            false,
            now,
        );
        let late = now + Duration::hours(24) - Duration::seconds(5);
        q.complete(
            id,
            Outcome::Retry {
                error: "offline".into(),
            },
            late,
        );
        assert_eq!(q.get(id).unwrap().state, MessageState::Expired);
        assert!(q.get(id).unwrap().next_attempt_at.is_none());
    }

    #[test]
    fn the_sweep_expires_what_ran_out_of_time_and_frees_the_payload() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        assert!(q.sweep(now).is_empty());

        let expired = q.sweep(now + Duration::hours(25));
        assert_eq!(expired, vec![id]);
        let message = q.get(id).unwrap();
        assert_eq!(message.state, MessageState::Expired);
        assert!(message.text.is_empty());
        assert!(
            q.sweep(now + Duration::hours(26)).is_empty(),
            "sweeping twice must not expire the same message again"
        );
    }

    #[test]
    fn a_site_that_returns_after_a_week_gets_everything_at_once() {
        let now = Utc::now();
        let mut q = Queue::new(24 * 30);
        for i in 0..5 {
            q.accept(
                site(1),
                site(2),
                "7025550100".into(),
                "4155550118".into(),
                format!("message {i}"),
                None,
                None,
                false,
                now + Duration::seconds(i),
            );
        }
        let due = q.due_for(site(2), now + Duration::days(7));
        assert_eq!(due.len(), 5);
        let texts: Vec<&str> = due.iter().map(|m| m.text.as_str()).collect();
        assert_eq!(
            texts,
            vec![
                "message 0",
                "message 1",
                "message 2",
                "message 3",
                "message 4"
            ]
        );
    }

    #[test]
    fn the_queue_depth_counts_only_what_is_still_owed() {
        let now = Utc::now();
        let (mut q, id) = queue_with_one(now);
        assert_eq!(q.queued_for(site(2)), 1);
        q.complete(id, Outcome::Delivered, now);
        assert_eq!(q.queued_for(site(2)), 0);
    }

    #[test]
    fn completing_an_unknown_message_is_a_no_op() {
        let now = Utc::now();
        let (mut q, _) = queue_with_one(now);
        q.complete(MessageId::new(), Outcome::Delivered, now);
        assert_eq!(q.len(), 1);
    }
}
