---
type: Rust Function
title: append_recur_ansi_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L774-L779
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances
---

# Signature

`pub(super) fn append_recur_ansi_string(value: &mut Vec<u8>, text: &str)`

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)
- [test_weekly_recur_blob_with_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances.md)