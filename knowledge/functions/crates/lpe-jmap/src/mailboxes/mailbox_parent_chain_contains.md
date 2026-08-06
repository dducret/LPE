---
type: Rust Function
title: mailbox_parent_chain_contains
resource: crates/lpe-jmap/src/mailboxes.rs#L775-L788
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
---

# Signature

`fn mailbox_parent_chain_contains( mailboxes: &HashMap<Uuid, &JmapMailbox>, start: Uuid, target: Uuid, ) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [validate_mailbox_set_names](../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)