---
type: Rust Method
title: parse
resource: crates/lpe-domain/src/mailbox_name.rs#L108-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse_with_reserved_policy
---

# Signature

`pub fn parse(value: impl AsRef<str>) -> Result<Self, MailboxNameError>`

# Calls

- [parse_with_reserved_policy](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse_with_reserved_policy.md)