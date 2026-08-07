---
type: Rust Method
title: load_account_identity_by_email_in_tx
resource: crates/lpe-storage/src/submission.rs#L1295-L1320
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
---

# Signature

`pub(crate) async fn load_account_identity_by_email_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, email: &str, ) -> Result<AccountIdentity>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [upsert_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)