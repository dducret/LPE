---
type: Rust Function
title: strict_finish_content_message
resource: crates/lpe-exchange/src/tests/mod.rs#L14212-L14263
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_finish_content_message( message: StrictContentMessageBuilder, message_changes: &mut Vec<StrictContentMessageChange>, ) -> Result<(), String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)