---
type: Rust Function
title: write_prefixed_bytes
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1255-L1258
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_visible_recipient_facts
---

# Signature

`fn write_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8])`

# Called by

- [fast_transfer_manifest_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [write_visible_recipient_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_visible_recipient_facts.md)