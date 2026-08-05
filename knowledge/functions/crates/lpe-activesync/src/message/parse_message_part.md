---
type: Rust Function
title: parse_message_part
resource: crates/lpe-activesync/src/message.rs#L66-L89
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/message/split_headers_and_body
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/message/parse_multipart_body
  called_by:
  - functions/crates/lpe-activesync/src/message/parse_mime_message
  - functions/crates/lpe-activesync/src/message/parse_multipart_body
---

# Signature

`fn parse_message_part(bytes: &[u8]) -> Result<ParsedMessagePart>`

# Calls

- [split_headers_and_body](../../../../../functions/crates/lpe-activesync/src/message/split_headers_and_body.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_multipart_body](../../../../../functions/crates/lpe-activesync/src/message/parse_multipart_body.md)

# Called by

- [parse_mime_message](../../../../../functions/crates/lpe-activesync/src/message/parse_mime_message.md)
- [parse_multipart_body](../../../../../functions/crates/lpe-activesync/src/message/parse_multipart_body.md)