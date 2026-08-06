use std::sync::atomic::{AtomicU64, Ordering};

static MAPI_CALENDAR_EVENT_DIRECT_COMMITTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_CALENDAR_EVENT_ICS_APPLIED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_CALENDAR_EVENT_ICS_IGNORED_OLDER_OR_SAME_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_CALENDAR_EVENT_ICS_KEPT_SERVER_CONTENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_CALENDAR_EVENT_DIRECT_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_CALENDAR_EVENT_ICS_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct MapiCalendarEventSaveMetrics {
    pub direct_committed_total: u64,
    pub ics_applied_total: u64,
    pub ics_ignored_older_or_same_total: u64,
    pub ics_kept_server_content_total: u64,
    pub direct_failed_total: u64,
    pub ics_failed_total: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum MapiCalendarEventSaveFlow {
    Direct,
    Ics,
}

#[derive(Clone, Copy)]
pub(crate) enum MapiCalendarEventSaveOutcome {
    Committed,
    IcsApplied,
    IcsIgnoredOlderOrSame,
    IcsKeptServerContent,
    Failed,
}

pub(crate) fn record_mapi_calendar_event_save(
    flow: MapiCalendarEventSaveFlow,
    outcome: MapiCalendarEventSaveOutcome,
) {
    match (flow, outcome) {
        (MapiCalendarEventSaveFlow::Direct, MapiCalendarEventSaveOutcome::Committed) => {
            MAPI_CALENDAR_EVENT_DIRECT_COMMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (MapiCalendarEventSaveFlow::Ics, MapiCalendarEventSaveOutcome::IcsApplied) => {
            MAPI_CALENDAR_EVENT_ICS_APPLIED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (MapiCalendarEventSaveFlow::Ics, MapiCalendarEventSaveOutcome::IcsIgnoredOlderOrSame) => {
            MAPI_CALENDAR_EVENT_ICS_IGNORED_OLDER_OR_SAME_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (MapiCalendarEventSaveFlow::Ics, MapiCalendarEventSaveOutcome::IcsKeptServerContent) => {
            MAPI_CALENDAR_EVENT_ICS_KEPT_SERVER_CONTENT_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (MapiCalendarEventSaveFlow::Direct, MapiCalendarEventSaveOutcome::Failed) => {
            MAPI_CALENDAR_EVENT_DIRECT_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (MapiCalendarEventSaveFlow::Ics, MapiCalendarEventSaveOutcome::Failed) => {
            MAPI_CALENDAR_EVENT_ICS_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

pub fn mapi_calendar_event_save_metrics() -> MapiCalendarEventSaveMetrics {
    MapiCalendarEventSaveMetrics {
        direct_committed_total: MAPI_CALENDAR_EVENT_DIRECT_COMMITTED_TOTAL.load(Ordering::Relaxed),
        ics_applied_total: MAPI_CALENDAR_EVENT_ICS_APPLIED_TOTAL.load(Ordering::Relaxed),
        ics_ignored_older_or_same_total: MAPI_CALENDAR_EVENT_ICS_IGNORED_OLDER_OR_SAME_TOTAL
            .load(Ordering::Relaxed),
        ics_kept_server_content_total: MAPI_CALENDAR_EVENT_ICS_KEPT_SERVER_CONTENT_TOTAL
            .load(Ordering::Relaxed),
        direct_failed_total: MAPI_CALENDAR_EVENT_DIRECT_FAILED_TOTAL.load(Ordering::Relaxed),
        ics_failed_total: MAPI_CALENDAR_EVENT_ICS_FAILED_TOTAL.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_event_save_metrics_preserve_direct_and_ics_outcomes() {
        let before = mapi_calendar_event_save_metrics();

        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Direct,
            MapiCalendarEventSaveOutcome::Committed,
        );
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Ics,
            MapiCalendarEventSaveOutcome::IcsApplied,
        );
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Ics,
            MapiCalendarEventSaveOutcome::IcsIgnoredOlderOrSame,
        );
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Ics,
            MapiCalendarEventSaveOutcome::IcsKeptServerContent,
        );
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Direct,
            MapiCalendarEventSaveOutcome::Failed,
        );
        record_mapi_calendar_event_save(
            MapiCalendarEventSaveFlow::Ics,
            MapiCalendarEventSaveOutcome::Failed,
        );

        let after = mapi_calendar_event_save_metrics();
        assert!(after.direct_committed_total >= before.direct_committed_total + 1);
        assert!(after.ics_applied_total >= before.ics_applied_total + 1);
        assert!(after.ics_ignored_older_or_same_total >= before.ics_ignored_older_or_same_total + 1);
        assert!(
            after.ics_kept_server_content_total >= before.ics_kept_server_content_total + 1
        );
        assert!(after.direct_failed_total >= before.direct_failed_total + 1);
        assert!(after.ics_failed_total >= before.ics_failed_total + 1);
    }
}
