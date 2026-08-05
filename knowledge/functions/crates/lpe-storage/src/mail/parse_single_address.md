---
type: Rust Function
title: parse_single_address
resource: crates/lpe-storage/src/mail.rs#L251-L278
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
---

# Signature

`fn parse_single_address(value: &str) -> Option<ParsedMailAddress>`

# Called by

- [parse_rfc822_message](../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)