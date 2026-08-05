---
type: Rust Function
title: format_rfc5322_date
resource: crates/lpe-imap/src/render.rs#L1106-L1126
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-imap/src/render/format_message_date
---

# Signature

`fn format_rfc5322_date(source: &str) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_message_date](../../../../../functions/crates/lpe-imap/src/render/format_message_date.md)