---
type: Rust Function
title: push_state_entries_to_types
resource: crates/lpe-jmap/src/state.rs#L412-L426
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`pub(crate) fn push_state_entries_to_types( entries: &[StateEntry], ) -> HashMap<String, HashMap<String, String>>`

# Calls

- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)