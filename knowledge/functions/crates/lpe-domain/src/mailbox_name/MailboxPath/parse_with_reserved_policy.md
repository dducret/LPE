---
type: Rust Method
title: parse_with_reserved_policy
resource: crates/lpe-domain/src/mailbox_name.rs#L116-L151
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/system
---

# Signature

`fn parse_with_reserved_policy( value: &str, reserved_policy: ReservedNamePolicy, ) -> Result<Self, MailboxNameError>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse.md)
- [system](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/system.md)