---
type: Rust Method
title: has_sender_right_in_tx
resource: crates/lpe-storage/src/submission.rs#L1257-L1285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/resolve_submission_authorization_in_tx
---

# Signature

`async fn has_sender_right_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, grantee_account_id: Uuid, sender_right: SenderDelegationRight, ) -> Result<bool>`

# Called by

- [resolve_submission_authorization_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/resolve_submission_authorization_in_tx.md)