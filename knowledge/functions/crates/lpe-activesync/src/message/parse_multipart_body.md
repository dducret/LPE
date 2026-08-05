---
type: Rust Function
title: parse_multipart_body
resource: crates/lpe-activesync/src/message.rs#L97-L130
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/message/parse_message_part
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/message/parse_message_part
---

# Signature

`fn parse_multipart_body(content_type: &str, body: &[u8]) -> Result<String>`

# Calls

- [parse_message_part](../../../../../functions/crates/lpe-activesync/src/message/parse_message_part.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_message_part](../../../../../functions/crates/lpe-activesync/src/message/parse_message_part.md)