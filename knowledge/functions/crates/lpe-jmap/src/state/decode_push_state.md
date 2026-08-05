---
type: Rust Function
title: decode_push_state
resource: crates/lpe-jmap/src/state.rs#L388-L410
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`pub(crate) fn decode_push_state(value: &str) -> Result<PushStateToken>`

# Called by

- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)