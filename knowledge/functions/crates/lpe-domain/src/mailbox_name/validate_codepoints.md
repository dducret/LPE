---
type: Rust Function
title: validate_codepoints
resource: crates/lpe-domain/src/mailbox_name.rs#L366-L376
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/is_private_use
  - functions/crates/lpe-domain/src/mailbox_name/is_unsafe_invisible
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment
  - functions/crates/lpe-domain/src/mailbox_name/validate_normalized_segment
---

# Signature

`fn validate_codepoints(value: &str) -> Result<(), MailboxNameError>`

# Calls

- [is_private_use](../../../../../functions/crates/lpe-domain/src/mailbox_name/is_private_use.md)
- [is_unsafe_invisible](../../../../../functions/crates/lpe-domain/src/mailbox_name/is_unsafe_invisible.md)

# Called by

- [validate_raw_segment](../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment.md)
- [validate_normalized_segment](../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_normalized_segment.md)