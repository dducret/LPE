---
type: Rust Function
title: trim_single_structural_crlf
resource: crates/lpe-storage/src/mail.rs#L280-L286
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_message_attachments
---

# Signature

`fn trim_single_structural_crlf(bytes: &mut Vec<u8>)`

# Called by

- [parse_message_attachments](../../../../../functions/crates/lpe-storage/src/mail/parse_message_attachments.md)