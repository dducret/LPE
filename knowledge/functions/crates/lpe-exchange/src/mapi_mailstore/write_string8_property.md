---
type: Rust Function
title: write_string8_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1371-L1377
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`fn write_string8_property(buffer: &mut Vec<u8>, property_tag: u32, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_normalized_subject_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property.md)
- [write_fast_transfer_special_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)