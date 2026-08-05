---
type: Rust Function
title: append_recur_wide_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L781-L787
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances
---

# Signature

`pub(super) fn append_recur_wide_string(value: &mut Vec<u8>, text: &str)`

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)
- [test_weekly_recur_blob_with_modified_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance.md)
- [test_weekly_recur_blob_with_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances.md)