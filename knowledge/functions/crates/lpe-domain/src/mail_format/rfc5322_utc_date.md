---
type: Rust Function
title: rfc5322_utc_date
resource: crates/lpe-domain/src/mail_format.rs#L60-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/sanitize_header_value
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-domain/src/mail_format/weekday_name
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
---

# Signature

`pub fn rfc5322_utc_date(value: &str) -> String`

# Calls

- [sanitize_header_value](../../../../../functions/crates/lpe-domain/src/mail_format/sanitize_header_value.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [weekday_name](../../../../../functions/crates/lpe-domain/src/mail_format/weekday_name.md)

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)