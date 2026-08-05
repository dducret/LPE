---
type: Rust Method
title: validate
resource: crates/lpe-domain/src/mailbox_name.rs#L64-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment
  - functions/crates/lpe-domain/src/mailbox_name/validate_normalized_segment
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/is_reserved_key
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name
  - functions/crates/lpe-domain/src/mailbox_name/has_mixed_script_confusable
---

# Signature

`fn validate( value: &str, reserved_policy: ReservedNamePolicy, ) -> Result<Self, MailboxNameError>`

# Calls

- [validate_raw_segment](../../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment.md)
- [validate_normalized_segment](../../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_normalized_segment.md)
- [is_reserved_key](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/is_reserved_key.md)
- [for_display_name](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name.md)
- [has_mixed_script_confusable](../../../../../../functions/crates/lpe-domain/src/mailbox_name/has_mixed_script_confusable.md)