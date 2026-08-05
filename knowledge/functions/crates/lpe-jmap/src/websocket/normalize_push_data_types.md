---
type: Rust Function
title: normalize_push_data_types
resource: crates/lpe-jmap/src/websocket.rs#L692-L703
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
  - functions/crates/lpe-jmap/src/websocket/push_enable_null_or_missing_data_types_subscribes_to_all_supported_types
---

# Signature

`pub(crate) fn normalize_push_data_types(data_types: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [handle_websocket_message](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
- [push_enable_null_or_missing_data_types_subscribes_to_all_supported_types](../../../../../functions/crates/lpe-jmap/src/websocket/push_enable_null_or_missing_data_types_subscribes_to_all_supported_types.md)