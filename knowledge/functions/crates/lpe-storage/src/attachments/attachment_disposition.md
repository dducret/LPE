---
type: Rust Function
title: attachment_disposition
resource: crates/lpe-storage/src/attachments.rs#L1154-L1159
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
---

# Signature

`fn attachment_disposition(value: Option<&str>) -> &'static str`

# Called by

- [insert_calendar_event_attachment_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [add_calendar_event_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)