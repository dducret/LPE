---
type: Rust Function
title: address_value
resource: crates/lpe-jmap/src/convert.rs#L114-L119
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/values/email_to_value
---

# Signature

`pub(crate) fn address_value(email: &str, name: Option<&str>) -> Value`

# Called by

- [email_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_to_value.md)