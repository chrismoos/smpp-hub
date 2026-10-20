use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrySchedule {
    steps: Vec<Duration>,
    tail: Duration,
}

impl Default for RetrySchedule {
    fn default() -> Self {
        Self {
            steps: vec![
                Duration::seconds(30),
                Duration::minutes(1),
                Duration::minutes(5),
                Duration::minutes(15),
                Duration::hours(1),
            ],
            tail: Duration::hours(1),
        }
    }
}

impl RetrySchedule {
    pub fn delay_for(&self, attempts: u32) -> Duration {
        let index = attempts.saturating_sub(1) as usize;
        *self.steps.get(index).unwrap_or(&self.tail)
    }
}

pub fn next_attempt(
    schedule: &RetrySchedule,
    attempts: u32,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let at = now + schedule.delay_for(attempts);
    (at < expires_at).then_some(at)
}

pub fn expiry(now: DateTime<Utc>, ttl_hours: u32) -> DateTime<Utc> {
    now + Duration::hours(i64::from(ttl_hours))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_retry_is_quick_because_most_sites_are_just_restarting() {
        let s = RetrySchedule::default();
        assert_eq!(s.delay_for(1), Duration::seconds(30));
        assert_eq!(s.delay_for(2), Duration::minutes(1));
    }

    #[test]
    fn the_backoff_flattens_to_hourly() {
        let s = RetrySchedule::default();
        assert_eq!(s.delay_for(5), Duration::hours(1));
        assert_eq!(s.delay_for(50), Duration::hours(1));
        assert_eq!(s.delay_for(u32::MAX), Duration::hours(1));
    }

    #[test]
    fn a_zeroth_attempt_uses_the_first_delay() {
        assert_eq!(
            RetrySchedule::default().delay_for(0),
            Duration::seconds(30),
            "an off-by-one here would skip the first backoff step"
        );
    }

    #[test]
    fn an_attempt_is_scheduled_while_the_message_still_has_time() {
        let now = Utc::now();
        let expires = expiry(now, 24);
        let at = next_attempt(&RetrySchedule::default(), 1, now, expires).unwrap();
        assert_eq!(at, now + Duration::seconds(30));
    }

    #[test]
    fn nothing_is_scheduled_past_the_ttl() {
        let now = Utc::now();
        let expires = now + Duration::seconds(10);
        assert_eq!(
            next_attempt(&RetrySchedule::default(), 1, now, expires),
            None,
            "a doomed message should not sit in the queue waiting to be swept"
        );
    }

    #[test]
    fn the_ttl_is_measured_from_acceptance() {
        let now = Utc::now();
        assert_eq!(expiry(now, 24), now + Duration::hours(24));
        assert_eq!(expiry(now, 1), now + Duration::hours(1));
    }
}
