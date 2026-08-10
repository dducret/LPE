---
type: Rust Function
title: merge_sync_predecessor_change_lists
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L31-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction
---

# Signature

`pub(super) fn merge_sync_predecessor_change_lists(first: &[u8], second: &[u8]) -> Result<Vec<u8>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [imported_event_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction.md)