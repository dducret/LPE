---
type: Rust Method
title: add_calendar_event_attachment
resource: crates/lpe-jmap/src/tests.rs#L1911-L1931
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn add_calendar_event_attachment( &self, _principal_account_id: Uuid, event_id: Uuid, attachment: AttachmentUploadInput, _audit: AuditEntryInput, ) -> Result<Option<CalendarEventAttachment>>`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [calendar_attachment_file_reference](../../../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)