---
type: Rust Function
title: calendar_attachment_file_reference
resource: crates/lpe-storage/src/attachments.rs#L1151-L1153
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_event
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/calendar_event_attachment_from_row
---

# Signature

`pub fn calendar_attachment_file_reference(event_id: Uuid, attachment_id: Uuid) -> String`

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update.md)
- [add_calendar_event_attachment](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/add_calendar_event_attachment.md)
- [insert_calendar_event_attachment_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx.md)
- [add_calendar_event_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [calendar_event_attachment_from_row](../../../../../functions/crates/lpe-storage/src/attachments/calendar_event_attachment_from_row.md)