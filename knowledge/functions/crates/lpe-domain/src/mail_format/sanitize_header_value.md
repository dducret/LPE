---
type: Rust Function
title: sanitize_header_value
resource: crates/lpe-domain/src/mail_format.rs#L9-L15
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/mail_format/quote_display_name
  - functions/crates/lpe-domain/src/mail_format/quote_header_parameter
  - functions/crates/lpe-domain/src/mail_format/format_mailbox_address
  - functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date
  - functions/crates/lpe-jmap/src/upload/push_header
---

# Signature

`pub fn sanitize_header_value(value: &str) -> String`

# Called by

- [quote_display_name](../../../../../functions/crates/lpe-domain/src/mail_format/quote_display_name.md)
- [quote_header_parameter](../../../../../functions/crates/lpe-domain/src/mail_format/quote_header_parameter.md)
- [format_mailbox_address](../../../../../functions/crates/lpe-domain/src/mail_format/format_mailbox_address.md)
- [rfc5322_utc_date](../../../../../functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date.md)
- [push_header](../../../../../functions/crates/lpe-jmap/src/upload/push_header.md)