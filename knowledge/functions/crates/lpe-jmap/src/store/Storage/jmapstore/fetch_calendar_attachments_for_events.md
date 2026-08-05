---
type: Rust Method
title: fetch_calendar_attachments_for_events
resource: crates/lpe-jmap/src/store.rs#L1035-L1042
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn fetch_calendar_attachments_for_events( &self, principal_account_id: Uuid, event_ids: &[Uuid], ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>>`