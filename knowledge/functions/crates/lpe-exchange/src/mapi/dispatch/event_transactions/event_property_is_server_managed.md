---
type: Rust Function
title: event_property_is_server_managed
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L337-L348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
---

# Signature

`fn event_property_is_server_managed(tag: u32) -> bool`

# Called by

- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)