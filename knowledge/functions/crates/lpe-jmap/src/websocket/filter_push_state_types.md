---
type: Rust Function
title: filter_push_state_types
resource: crates/lpe-jmap/src/websocket.rs#L742-L760
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`fn filter_push_state_types( type_states: HashMap<String, HashMap<String, String>>, enabled_types: &HashSet<String>, ) -> HashMap<String, HashMap<String, String>>`

# Called by

- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)