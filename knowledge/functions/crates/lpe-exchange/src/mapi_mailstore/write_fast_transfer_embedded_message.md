---
type: Rust Function
title: write_fast_transfer_embedded_message
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1173-L1182
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
---

# Signature

`fn write_fast_transfer_embedded_message(buffer: &mut Vec<u8>, attachment: &AttachmentSyncFact)`

# Calls

- [write_utf16_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)

# Called by

- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)