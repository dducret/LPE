---
type: Rust Function
title: default_embedded_message_properties
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1363-L1368
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_blob
---

# Signature

`fn default_embedded_message_properties() -> HashMap<u32, MapiValue>`

# Called by

- [open_embedded_message_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source.md)
- [embedded_message_properties_from_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_properties_from_blob.md)