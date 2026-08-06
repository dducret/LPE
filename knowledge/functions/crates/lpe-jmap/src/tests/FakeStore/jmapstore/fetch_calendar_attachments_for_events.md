---
type: Rust Method
title: fetch_calendar_attachments_for_events
resource: crates/lpe-jmap/src/tests.rs#L1894-L1909
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn fetch_calendar_attachments_for_events( &self, _principal_account_id: Uuid, event_ids: &[Uuid], ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)