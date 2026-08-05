---
type: Rust Function
title: quote_header_parameter
resource: crates/lpe-domain/src/mail_format.rs#L29-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/sanitize_header_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/quote_mime_parameter
---

# Signature

`pub fn quote_header_parameter(value: &str) -> String`

# Calls

- [sanitize_header_value](../../../../../functions/crates/lpe-domain/src/mail_format/sanitize_header_value.md)

# Called by

- [quote_mime_parameter](../../../../../functions/crates/lpe-exchange/src/service/ews/mime/quote_mime_parameter.md)