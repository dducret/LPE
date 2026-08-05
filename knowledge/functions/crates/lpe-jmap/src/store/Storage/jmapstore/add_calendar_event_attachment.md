---
type: Rust Method
title: add_calendar_event_attachment
resource: crates/lpe-jmap/src/store.rs#L1044-L1053
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn add_calendar_event_attachment( &self, principal_account_id: Uuid, event_id: Uuid, attachment: AttachmentUploadInput, audit: AuditEntryInput, ) -> Result<Option<CalendarEventAttachment>>`