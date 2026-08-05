---
type: Rust Function
title: stage_clearable_event_property_deletion
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L447-L499
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
---

# Signature

`fn stage_clearable_event_property_deletion( transaction: &mut MapiEventTransaction, storage_tag: u32, ) -> bool`

# Called by

- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)