---
type: Rust Method
title: push_categories
resource: crates/lpe-jmap/src/websocket.rs#L578-L611
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`pub(crate) fn push_categories( &self, data_types: &HashSet<String>, ) -> Vec<CanonicalChangeCategory>`

# Calls

- [is_mail_push_type](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [handle_websocket](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket.md)
- [recover_push_enable_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)