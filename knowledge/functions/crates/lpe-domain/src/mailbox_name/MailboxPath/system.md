---
type: Rust Method
title: system
resource: crates/lpe-domain/src/mailbox_name.rs#L112-L114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse_with_reserved_policy
---

# Signature

`pub fn system(value: impl AsRef<str>) -> Result<Self, MailboxNameError>`

# Calls

- [parse_with_reserved_policy](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse_with_reserved_policy.md)