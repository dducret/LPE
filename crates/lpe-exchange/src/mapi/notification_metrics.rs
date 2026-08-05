use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static MAPI_NOTIFICATION_WAIT_EVENT_PENDING_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_NOTIFICATION_WAIT_IDLE_TIMEOUT_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_NOTIFICATION_WAIT_SESSION_UNAVAILABLE_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_NOTIFICATION_WAIT_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_NOTIFICATION_WAIT_ELAPSED_MILLISECONDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_NOTIFICATION_NEW_MAIL_DELIVERIES_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct MapiNotificationMetrics {
    pub wait_event_pending_total: u64,
    pub wait_idle_timeout_total: u64,
    pub wait_session_unavailable_total: u64,
    pub wait_error_total: u64,
    pub wait_elapsed_milliseconds_total: u64,
    pub new_mail_deliveries_total: u64,
}

pub(crate) enum MapiNotificationWaitOutcome {
    EventPending,
    IdleTimeout,
    SessionUnavailable,
    Error,
}

pub(crate) fn record_mapi_notification_wait_completion(
    outcome: MapiNotificationWaitOutcome,
    elapsed: Duration,
) {
    match outcome {
        MapiNotificationWaitOutcome::EventPending => {
            MAPI_NOTIFICATION_WAIT_EVENT_PENDING_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        MapiNotificationWaitOutcome::IdleTimeout => {
            MAPI_NOTIFICATION_WAIT_IDLE_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        MapiNotificationWaitOutcome::SessionUnavailable => {
            MAPI_NOTIFICATION_WAIT_SESSION_UNAVAILABLE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        MapiNotificationWaitOutcome::Error => {
            MAPI_NOTIFICATION_WAIT_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
    MAPI_NOTIFICATION_WAIT_ELAPSED_MILLISECONDS_TOTAL
        .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
}

pub(crate) fn record_mapi_new_mail_notification_deliveries(delivery_count: usize) {
    MAPI_NOTIFICATION_NEW_MAIL_DELIVERIES_TOTAL.fetch_add(delivery_count as u64, Ordering::Relaxed);
}

pub fn mapi_notification_metrics() -> MapiNotificationMetrics {
    MapiNotificationMetrics {
        wait_event_pending_total: MAPI_NOTIFICATION_WAIT_EVENT_PENDING_TOTAL
            .load(Ordering::Relaxed),
        wait_idle_timeout_total: MAPI_NOTIFICATION_WAIT_IDLE_TIMEOUT_TOTAL.load(Ordering::Relaxed),
        wait_session_unavailable_total: MAPI_NOTIFICATION_WAIT_SESSION_UNAVAILABLE_TOTAL
            .load(Ordering::Relaxed),
        wait_error_total: MAPI_NOTIFICATION_WAIT_ERROR_TOTAL.load(Ordering::Relaxed),
        wait_elapsed_milliseconds_total: MAPI_NOTIFICATION_WAIT_ELAPSED_MILLISECONDS_TOTAL
            .load(Ordering::Relaxed),
        new_mail_deliveries_total: MAPI_NOTIFICATION_NEW_MAIL_DELIVERIES_TOTAL
            .load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_metrics_record_wait_completion_and_new_mail_delivery() {
        let before = mapi_notification_metrics();

        record_mapi_notification_wait_completion(
            MapiNotificationWaitOutcome::EventPending,
            Duration::from_millis(7),
        );
        record_mapi_new_mail_notification_deliveries(1);

        let after = mapi_notification_metrics();
        assert!(after.wait_event_pending_total >= before.wait_event_pending_total + 1);
        assert!(
            after.wait_elapsed_milliseconds_total >= before.wait_elapsed_milliseconds_total + 7
        );
        assert!(after.new_mail_deliveries_total >= before.new_mail_deliveries_total + 1);
    }
}
