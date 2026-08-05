---
type: Rust Function
title: format_mailbox_address
resource: crates/lpe-domain/src/mail_format.rs#L35-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/sanitize_header_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mime/render_mime_address
  - functions/crates/lpe-jmap/src/upload/header_address
---

# Signature

`pub fn format_mailbox_address( address: &str, display_name: Option<&str>, policy: DisplayNamePolicy, ) -> String`

# Calls

- [sanitize_header_value](../../../../../functions/crates/lpe-domain/src/mail_format/sanitize_header_value.md)

# Called by

- [render_mime_address](../../../../../functions/crates/lpe-exchange/src/service/ews/mime/render_mime_address.md)
- [header_address](../../../../../functions/crates/lpe-jmap/src/upload/header_address.md)