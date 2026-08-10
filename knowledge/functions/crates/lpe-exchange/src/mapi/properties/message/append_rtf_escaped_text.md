---
type: Rust Function
title: append_rtf_escaped_text
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L332-L351
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
---

# Signature

`fn append_rtf_escaped_text(output: &mut String, value: &str)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [uncompressed_rtf_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)