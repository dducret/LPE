---
type: Rust Function
title: calendar_change_key
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L4-L6
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
---

# Signature

`pub(in crate::service) fn calendar_change_key(event: &AccessibleEvent, version: &str) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [event_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)