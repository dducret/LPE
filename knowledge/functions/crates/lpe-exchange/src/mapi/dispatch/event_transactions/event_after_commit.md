---
type: Rust Function
title: event_after_commit
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L620-L646
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`pub(super) fn event_after_commit( mut event: lpe_storage::AccessibleEvent, input: Option<&lpe_storage::UpsertClientEventInput>, ) -> lpe_storage::AccessibleEvent`

# Called by

- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)