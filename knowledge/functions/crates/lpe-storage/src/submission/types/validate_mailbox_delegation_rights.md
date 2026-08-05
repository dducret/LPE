---
type: Rust Function
title: validate_mailbox_delegation_rights
resource: crates/lpe-storage/src/submission/types.rs#L273-L289
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant
---

# Signature

`pub(super) fn validate_mailbox_delegation_rights( may_read: bool, may_write: bool, may_delete: bool, may_share: bool, ) -> Result<()>`

# Called by

- [set_mailbox_folder_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant.md)