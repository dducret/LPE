---
type: Rust Function
title: body_value
resource: crates/lpe-jmap/src/mail/values.rs#L467-L485
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/values/email_to_value
---

# Signature

`fn body_value(value: &str, max_bytes: Option<usize>) -> (String, bool)`

# Called by

- [email_to_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_to_value.md)