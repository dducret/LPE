---
type: Rust Function
title: imported_version_wins_last_writer
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L49-L66
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction
---

# Signature

`pub(super) fn imported_version_wins_last_writer( incoming_last_modification_time: u64, incoming_change_key: &[u8], current_last_modification_time: u64, current_change_key: &[u8], ) -> Result<bool>`

# Called by

- [imported_event_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction.md)