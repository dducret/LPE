---
type: Rust Function
title: parse_calendar_attachment_file_reference
resource: crates/lpe-storage/src/attachments.rs#L1128-L1139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_blob
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
---

# Signature

`pub fn parse_calendar_attachment_file_reference(value: &str) -> Option<(Uuid, Uuid)>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [delete_calendar_event_attachment](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_calendar_event_attachment.md)
- [fetch_calendar_attachment_blob](../../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_blob.md)
- [delete_calendar_event_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)