---
type: Rust Function
title: trim_line_end
resource: crates/lpe-imap/src/service.rs#L149-L154
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/service/decode_command_line
  - functions/crates/lpe-imap/src/service/command_tag_from_bytes
---

# Signature

`fn trim_line_end(mut bytes: &[u8]) -> &[u8]`

# Called by

- [decode_command_line](../../../../../functions/crates/lpe-imap/src/service/decode_command_line.md)
- [command_tag_from_bytes](../../../../../functions/crates/lpe-imap/src/service/command_tag_from_bytes.md)