---
type: Rust Function
title: parse_mail_datetime
resource: crates/lpe-storage/src/mail.rs#L176-L186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_message_date_header
---

# Signature

`fn parse_mail_datetime(value: &str) -> Option<String>`

# Called by

- [parse_message_date_header](../../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header.md)