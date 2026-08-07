---
type: Rust Function
title: calendar_event_attachment_from_row
resource: crates/lpe-storage/src/attachments.rs#L1072-L1085
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachments_for_events
---

# Signature

`fn calendar_event_attachment_from_row( row: sqlx::postgres::PgRow, ) -> Result<CalendarEventAttachment>`

# Calls

- [calendar_attachment_file_reference](../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)

# Called by

- [fetch_calendar_attachments_for_events](../../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachments_for_events.md)