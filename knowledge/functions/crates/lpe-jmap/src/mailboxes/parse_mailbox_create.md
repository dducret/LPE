---
type: Rust Function
title: parse_mailbox_create
resource: crates/lpe-jmap/src/mailboxes.rs#L578-L600
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
---

# Signature

`fn parse_mailbox_create(value: Value) -> Result<MailboxCreateInput>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_parent_id_field](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field.md)

# Called by

- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)