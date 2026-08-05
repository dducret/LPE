---
type: Rust Method
title: for_display_name
resource: crates/lpe-domain/src/mailbox_name.rs#L167-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/fold_for_comparison
  - functions/crates/lpe-domain/src/mailbox_name/confusable_skeleton
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/canonical_key
  - functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/validate
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_key
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name
---

# Signature

`pub fn for_display_name(value: &str) -> Self`

# Calls

- [fold_for_comparison](../../../../../../functions/crates/lpe-domain/src/mailbox_name/fold_for_comparison.md)
- [confusable_skeleton](../../../../../../functions/crates/lpe-domain/src/mailbox_name/confusable_skeleton.md)

# Called by

- [canonical_key](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/canonical_key.md)
- [validate](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/validate.md)
- [canonical_key](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_key.md)
- [system_role_for_display_name](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)