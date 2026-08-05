---
type: Rust Function
title: validate_raw_segment
resource: crates/lpe-domain/src/mailbox_name.rs#L340-L354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/has_ascii_boundary_whitespace
  - functions/crates/lpe-domain/src/mailbox_name/validate_codepoints
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/validate
---

# Signature

`fn validate_raw_segment(value: &str) -> Result<(), MailboxNameError>`

# Calls

- [has_ascii_boundary_whitespace](../../../../../functions/crates/lpe-domain/src/mailbox_name/has_ascii_boundary_whitespace.md)
- [validate_codepoints](../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_codepoints.md)

# Called by

- [validate](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/validate.md)