---
type: Rust Method
title: emit_mail_delegation_change
resource: crates/lpe-storage/src/change.rs#L265-L290
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant
---

# Signature

`pub(crate) async fn emit_mail_delegation_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, grantee_account_id: Uuid, ) -> Result<()>`

# Calls

- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [upsert_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)
- [set_mailbox_folder_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant.md)