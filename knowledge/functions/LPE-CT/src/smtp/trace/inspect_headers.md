---
type: Rust Function
title: inspect_headers
resource: LPE-CT/src/smtp/trace.rs#L152-L173
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
---

# Signature

`fn inspect_headers(data: &[u8]) -> Vec<(String, String)>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)