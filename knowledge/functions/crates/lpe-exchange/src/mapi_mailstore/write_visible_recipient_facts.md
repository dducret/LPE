---
type: Rust Function
title: write_visible_recipient_facts
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1067-L1092
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_prefixed_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
---

# Signature

`fn write_visible_recipient_facts(buffer: &mut Vec<u8>, email: &JmapEmail)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_prefixed_bytes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_prefixed_bytes.md)

# Called by

- [fast_transfer_manifest_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)